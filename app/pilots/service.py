from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.pilots.repository import PilotRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class PilotOperationsService:
    """管理试点方案、配额、体验场次租约、观察记录版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = PilotRepository(self.connection)

    def list_protocols(self) -> list[dict[str, Any]]:
        return self.repository.active_protocols()

    def create_protocol(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            if repository.protocol_by_code(payload["code"]):
                raise ConflictError("参数方案编码已存在")
            return repository.create_protocol(
                code=payload["code"], name=payload["name"], capability=payload["capability"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return PilotRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            protocol = repository.protocol_by_code(payload["protocol_code"])
            if protocol is None or not protocol["active"]:
                raise NotFoundError("参数方案不存在或已经停用")
            parameters = self._validate_parameters(protocol, payload["parameters"])
            existing = repository.session_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的试点参数")
                return dict(repository.session_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_session(
                protocol_id=protocol["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=protocol["max_attempts"], now=now,
            )

    def list_sessions(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_sessions(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_session(self, session_id: int) -> dict[str, Any]:
        row = self.repository.session_by_id(session_id)
        if row is None:
            raise NotFoundError("试点体验场次不存在")
        observation = dict(row)
        observation["observations"] = self.repository.observation_versions(session_id)
        observation["interventions"] = self.repository.interventions(session_id)
        return observation

    def claim(self, site_code: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any]:
        """在一个即时事务内完成领取：场地状态、能力清单与并发上限共同生效。

        正在运行（running）与等待安全停止（cancel_requested）的场次都占用容量；
        场地暂停或关闭、不具备所声明能力或容量已满时拒绝领取，排队场次保持不动。
        """
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            site = repository.site_by_code(site_code)
            if site is None:
                raise NotFoundError("领取场地不存在", context={"reason": "site_not_found", "site_code": site_code})
            site_capabilities = json.loads(site["capabilities_json"])
            active_sessions = repository.count_site_occupancy(site_code)
            capacity = int(site["max_concurrent"])
            snapshot = self._site_snapshot(site, site_capabilities, active_sessions)
            if site["status"] == "suspended":
                raise ConflictError("场地已暂停，不能领取场次", context={**snapshot, "reason": "site_suspended"})
            if site["status"] == "closed":
                raise ConflictError("场地已关闭，不能领取场次", context={**snapshot, "reason": "site_closed"})
            requested = sorted(set(capabilities))
            offered = set(site_capabilities)
            effective_capabilities = sorted(offered) if not requested else [item for item in requested if item in offered]
            if not effective_capabilities:
                detail = "场地不具备所声明的能力，不能领取场次" if requested else "场地未登记任何体验能力，不能领取场次"
                raise ConflictError(
                    detail,
                    context={**snapshot, "reason": "capability_not_offered", "requested_capabilities": requested},
                )
            if active_sessions >= capacity:
                raise ConflictError(
                    "场地并发容量已满，不能领取场次",
                    context={**snapshot, "reason": "site_capacity_full"},
                )
            candidate = repository.queued_candidate(effective_capabilities, now)
            if candidate is None:
                return {"session": None, **snapshot, "reason": "no_matching_session"}
            cursor = connection.execute(
                "UPDATE pilot_sessions SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (site_code, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                raise ConflictError("候选场次已被其他领取事务取走", context={**snapshot, "reason": "session_taken"})
            session = dict(repository.session_by_id(candidate["id"]))
            return {"session": session, **self._site_snapshot(site, site_capabilities, active_sessions + 1), "reason": "claimed"}

    @staticmethod
    def _site_snapshot(site: sqlite3.Row, site_capabilities: list[str], active_sessions: int) -> dict[str, Any]:
        capacity = int(site["max_concurrent"])
        return {
            "site": {
                "site_code": site["code"],
                "status": site["status"],
                "capabilities": site_capabilities,
                "max_concurrent": capacity,
                "active_sessions": active_sessions,
                "available_slots": max(0, capacity - active_sessions) if site["status"] == "active" else 0,
            },
        }

    def heartbeat(self, session_id: int, site_code: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE pilot_sessions SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, session_id, site_code),
            )
            if cursor.rowcount != 1:
                raise ConflictError("体验场次未由当前执行站点持有")
            return dict(PilotRepository(connection).session_by_id(session_id))

    def complete(self, session_id: int, site_code: str, observation: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("试点体验场次不存在")
            if session["status"] not in {"running", "cancel_requested"} or session["lease_owner"] != site_code:
                raise ConflictError("体验场次未由当前执行站点持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM pilot_observations WHERE session_id=?", (session_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO pilot_observations(session_id,version,observation_json,metrics_json,observation_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (session_id, version, json.dumps(observation, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"observation": observation, "metrics": metrics}), site_code, now),
            )
            terminal_status = "cancelled" if session["status"] == "cancel_requested" else "succeeded"
            connection.execute(
                "UPDATE pilot_sessions SET status=?,current_observation_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (terminal_status, version, now, now, session_id),
            )
            return dict(repository.session_by_id(session_id))

    def fail(self, session_id: int, site_code: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("试点体验场次不存在")
            if session["status"] not in {"running", "cancel_requested"} or session["lease_owner"] != site_code:
                raise ConflictError("体验场次未由当前执行站点持有")
            cancel_requested = session["status"] == "cancel_requested"
            can_retry = not cancel_requested and retryable and int(session["attempt_count"]) < int(session["max_attempts"])
            status = "queued" if can_retry else ("cancelled" if cancel_requested else "failed")
            delay = min(300, 2 ** max(0, int(session["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay)) if can_retry else now
            finished_at = None if can_retry else now
            connection.execute(
                "UPDATE pilot_sessions SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code if not cancel_requested else "cancelled", message[:2000], finished_at, now, session_id),
            )
            return dict(repository.session_by_id(session_id))

    def cancel(self, session_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(session_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, session_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
            if session["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消体验场次可以人工重试")
            chosen = session["priority"] if priority is None else priority
            connection.execute("UPDATE pilot_sessions SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, session["id"]))
        return self._intervene(session_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, session_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
            if session["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的体验场次可以调整优先级")
            connection.execute("UPDATE pilot_sessions SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, session["id"]))
        return self._intervene(session_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "session_ids": payload["session_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for session_id in list(dict.fromkeys(payload["session_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(session_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(session_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(session_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"session_id": session_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"session_id": session_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-site") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            rows = connection.execute(
                "SELECT * FROM pilot_sessions WHERE status IN ('running','cancel_requested') AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id",
                (now,),
            ).fetchall()
            for session in rows:
                before = dict(session)
                if session["status"] == "cancel_requested":
                    # 已请求安全停止且租约到期：尊重取消决定，立即释放容量。
                    status, finished_at, error_code = "cancelled", now, "cancelled"
                    cancelled.append(int(session["id"]))
                elif int(session["attempt_count"]) < int(session["max_attempts"]):
                    status, finished_at, error_code = "queued", None, "lease_expired"
                    recovered.append(int(session["id"]))
                else:
                    status, finished_at, error_code = "failed", now, "lease_expired"
                    exhausted.append(int(session["id"]))
                connection.execute(
                    "UPDATE pilot_sessions SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code=?,last_error_message='执行站点租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, error_code, finished_at, now, session["id"]),
                )
                after = dict(repository.session_by_id(session["id"]))
                repository.add_intervention(session_id=session["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM pilot_sessions GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM pilot_sessions WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "protocols": len(self.repository.active_protocols())}

    def _intervene(self, session_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("试点体验场次不存在")
            before = dict(session)
            mutation(connection, session, now)
            after = dict(repository.session_by_id(session_id))
            repository.add_intervention(session_id=session_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
        if session["status"] not in {"queued", "running"}:
            raise ConflictError("当前体验场次状态不允许取消")
        status = "cancel_requested" if session["status"] == "running" else "cancelled"
        connection.execute("UPDATE pilot_sessions SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, session["id"]))

    def _check_quota(self, repository: PilotRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队体验场次配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行体验场次配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数方案至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, protocol: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(protocol["parameter_schema_json"])
        values = {**json.loads(protocol["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含方案未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized


