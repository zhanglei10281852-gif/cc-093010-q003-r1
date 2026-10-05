from __future__ import annotations

from datetime import UTC, datetime

from app.catalog.service import CatalogService
from app.pilots.service import PilotOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection


SITE_BASE = {
    "name": "展会体验点",
    "site_type": "展会体验点",
    "region": "杭州",
}


def register_site(code: str, capabilities: list[str], *, max_concurrent: int = 1, status: str = "active", clock=None) -> dict:
    service = CatalogService(get_connection(), clock)
    payload = {**SITE_BASE, "code": code, "capabilities": capabilities, "max_concurrent": max_concurrent}
    site = service.create_site(payload)
    if status != "active":
        site = service.update_site(code, {"status": status})
    return site


PROTOCOL = {
    "code": "gait-assist",
    "name": "外骨骼步态体验方案",
    "capability": "gait-assist",
    "parameter_schema": {
        "minutes": {"type": "integer", "required": True, "minimum": 1, "maximum": 30},
        "assist_level": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "scene": {"type": "string", "required": True, "choices": ["stairs", "flat"]},
    },
    "default_parameters": {"assist_level": 0.4},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "pilot-operator-1", priority: int = 50) -> dict:
    return {
        "protocol_code": "gait-assist",
        "project_code": "expo-health-a",
        "requested_by": user,
        "parameters": {"minutes": 8, "scene": "stairs"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_protocol(client) -> None:
    response = client.post("/api/pilots/protocols?actor=administrator", json=PROTOCOL)
    assert response.status_code == 201, response.text


def test_protocol_submission_idempotency_and_parameter_validation(client):
    create_protocol(client)
    first = client.post("/api/pilots/sessions", json=submit_payload("request-000001"))
    second = client.post("/api/pilots/sessions", json=submit_payload("request-000001"))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    invalid = submit_payload("request-000002")
    invalid["parameters"]["minutes"] = 50
    rejected = client.post("/api/pilots/sessions", json=invalid)
    assert rejected.status_code == 422


def test_priority_capability_claim_and_observation_version(client):
    create_protocol(client)
    register_site("w0", ["other"])
    register_site("w1", ["gait-assist"])
    low = client.post("/api/pilots/sessions", json=submit_payload("priority-low", priority=10)).json()
    high = client.post("/api/pilots/sessions", json=submit_payload("priority-high", priority=90)).json()
    no_match = client.post("/api/pilots/sessions/claim", json={"site_code": "w0", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert no_match.status_code == 409
    assert no_match.json()["error"]["context"]["reason"] == "capability_not_offered"
    claimed = client.post("/api/pilots/sessions/claim", json={"site_code": "w1", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert claimed.status_code == 200
    assert claimed.json()["session"]["id"] == high["id"]
    completed = client.post(
        f"/api/pilots/sessions/{high['id']}/complete",
        json={"site_code": "w1", "observation": {"value": 3.14}, "metrics": {"seconds": 2}},
    )
    assert completed.status_code == 200
    details = client.get(f"/api/pilots/session-details/{high['id']}").json()
    assert details["status"] == "succeeded"
    assert details["current_observation_version"] == 1
    assert len(details["observations"]) == 1
    assert low["status"] == "queued"


def test_quota_cancel_retry_priority_and_batch_interventions(client):
    create_protocol(client)
    quota = client.put(
        "/api/pilots/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "limited", "max_queued": 1, "max_running": 1, "daily_submissions": 2},
    )
    assert quota.status_code == 200
    one = client.post("/api/pilots/sessions", json=submit_payload("quota-one", user="limited")).json()
    blocked = client.post("/api/pilots/sessions", json=submit_payload("quota-two", user="limited"))
    assert blocked.status_code == 409
    cancelled = client.post(f"/api/pilots/sessions/{one['id']}/cancel", json={"actor": "administrator", "reason": "项目暂停"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    retried = client.post(f"/api/pilots/sessions/{one['id']}/retry", json={"actor": "administrator", "reason": "项目恢复", "priority": 95})
    assert retried.status_code == 200 and retried.json()["priority"] == 95
    other = client.post("/api/pilots/sessions", json=submit_payload("batch-other", user="other-user")).json()
    batch = client.post(
        "/api/pilots/sessions/batch",
        json={"session_ids": [one["id"], other["id"]], "operation": "priority", "actor": "administrator", "reason": "临床合作方临时到场", "priority": 99},
    )
    assert batch.status_code == 200
    assert len(batch.json()["succeeded"]) == 2
    details = client.get(f"/api/pilots/session-details/{one['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel", "retry", "priority"]


def test_failure_backoff_and_expired_lease_recovery(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    register_site("site-a", ["gait-assist"], max_concurrent=1, clock=clock)
    first = service.submit(submit_payload("failure-000001"))
    claimed = service.claim("site-a", ["gait-assist"], 10)
    assert claimed["session"] and claimed["session"]["id"] == first["id"]
    assert claimed["site"]["active_sessions"] == 1
    failed = service.fail(first["id"], "site-a", "sensor_unstable", "步态传感器读数不稳定", True)
    assert failed["status"] == "queued"
    assert failed["available_at"] > failed["updated_at"]
    clock.advance(seconds=2)
    claimed_again = service.claim("site-a", ["gait-assist"], 10)
    assert claimed_again["session"] and claimed_again["session"]["attempt_count"] == 2
    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["exhausted"] == [first["id"]]
    details = service.get_session(first["id"])
    assert details["status"] == "failed"
    assert details["interventions"][-1]["action"] == "lease_recovery"


def test_single_capacity_site_never_holds_two_active_sessions(client):
    create_protocol(client)
    register_site("single", ["gait-assist"], max_concurrent=1)
    first = client.post("/api/pilots/sessions", json=submit_payload("cap-one")).json()
    client.post("/api/pilots/sessions", json=submit_payload("cap-two"))

    claimed = client.post("/api/pilots/sessions/claim", json={"site_code": "single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert claimed.status_code == 200
    body = claimed.json()
    assert body["session"]["id"] == first["id"]
    assert body["site"]["active_sessions"] == 1
    assert body["site"]["available_slots"] == 0

    # 第二位参与者到达：设备仍被占用，必须拒绝并给出原因与当前占用。
    rejected = client.post("/api/pilots/sessions/claim", json={"site_code": "single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert rejected.status_code == 409
    error = rejected.json()["error"]
    assert error["context"]["reason"] == "site_capacity_full"
    assert error["context"]["site"]["active_sessions"] == 1
    assert error["context"]["site"]["max_concurrent"] == 1
    assert error["context"]["site"]["available_slots"] == 0

    # 失败请求不能拿走队列项：第二场仍排队，且单容量场地只持有一个有效场次。
    queued = client.get("/api/pilots/sessions?status=queued").json()["items"]
    assert [item["id"] for item in queued] == [first["id"] + 1]
    running = client.get("/api/pilots/sessions?status=running").json()["items"]
    assert len(running) == 1 and running[0]["id"] == first["id"]

    # 场次完成后名额立即重新计算，第二场可被领取。
    done = client.post(
        f"/api/pilots/sessions/{first['id']}/complete",
        json={"site_code": "single", "observation": {"ok": True}, "metrics": {}},
    )
    assert done.status_code == 200
    second = client.post("/api/pilots/sessions/claim", json={"site_code": "single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert second.status_code == 200
    assert second.json()["session"]["id"] == first["id"] + 1
    assert second.json()["site"]["available_slots"] == 0


def test_suspended_and_closed_sites_cannot_claim(client):
    create_protocol(client)
    register_site("paused", ["gait-assist"], status="suspended")
    register_site("shut", ["gait-assist"], status="closed")
    client.post("/api/pilots/sessions", json=submit_payload("site-state"))

    for code, reason in [("paused", "site_suspended"), ("shut", "site_closed")]:
        response = client.post("/api/pilots/sessions/claim", json={"site_code": code, "capabilities": ["gait-assist"], "lease_seconds": 60})
        assert response.status_code == 409
        assert response.json()["error"]["context"]["reason"] == reason
        assert response.json()["error"]["context"]["site"]["available_slots"] == 0

    # 恢复运营后可以正常领取。
    CatalogService(get_connection()).update_site("paused", {"status": "active"})
    ok = client.post("/api/pilots/sessions/claim", json={"site_code": "paused", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert ok.status_code == 200 and ok.json()["session"] is not None


def test_capacity_expansion_recomputes_available_slots(client):
    create_protocol(client)
    register_site("grow", ["gait-assist"], max_concurrent=1)
    catalog = CatalogService(get_connection())
    client.post("/api/pilots/sessions", json=submit_payload("grow-one"))
    client.post("/api/pilots/sessions", json=submit_payload("grow-two"))

    first = client.post("/api/pilots/sessions/claim", json={"site_code": "grow", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert first.status_code == 200
    blocked = client.post("/api/pilots/sessions/claim", json={"site_code": "grow", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert blocked.status_code == 409 and blocked.json()["error"]["context"]["reason"] == "site_capacity_full"

    # 场地扩容后名额立即按真实状态重新计算。
    catalog.update_site("grow", {"max_concurrent": 2})
    second = client.post("/api/pilots/sessions/claim", json={"site_code": "grow", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert second.status_code == 200
    assert second.json()["site"]["active_sessions"] == 2
    assert second.json()["site"]["available_slots"] == 0


def test_cancel_requested_session_still_occupies_capacity(client):
    create_protocol(client)
    register_site("single", ["gait-assist"], max_concurrent=1)
    held = client.post("/api/pilots/sessions", json=submit_payload("held-0001")).json()
    client.post("/api/pilots/sessions", json=submit_payload("waiting-1"))
    client.post("/api/pilots/sessions/claim", json={"site_code": "single", "capabilities": ["gait-assist"], "lease_seconds": 60})

    cancelled = client.post(f"/api/pilots/sessions/{held['id']}/cancel", json={"actor": "manager", "reason": "等待安全停止"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancel_requested"

    blocked = client.post("/api/pilots/sessions/claim", json={"site_code": "single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert blocked.status_code == 409
    assert blocked.json()["error"]["context"]["reason"] == "site_capacity_full"
    assert blocked.json()["error"]["context"]["site"]["active_sessions"] == 1

    # 站点完成安全停止后，场次转为已取消并立即释放名额，排队场次可领取。
    finished = client.post(
        f"/api/pilots/sessions/{held['id']}/complete",
        json={"site_code": "single", "observation": {"stopped": True}, "metrics": {}},
    )
    assert finished.status_code == 200 and finished.json()["status"] == "cancelled"
    freed = client.post("/api/pilots/sessions/claim", json={"site_code": "single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert freed.status_code == 200
    assert freed.json()["session"]["id"] != held["id"]
    assert freed.json()["site"]["available_slots"] == 0


def test_cancel_requested_lease_expiry_releases_capacity(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 6, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    register_site("site-a", ["gait-assist"], max_concurrent=1, clock=clock)
    held = service.submit(submit_payload("cancel-hold"))
    next_one = service.submit(submit_payload("cancel-next"))
    assert service.claim("site-a", ["gait-assist"], 10)["session"]["id"] == held["id"]
    assert service.cancel(held["id"], "manager", "请求安全停止")["status"] == "cancel_requested"
    import pytest

    with pytest.raises(ConflictError) as blocked:
        service.claim("site-a", ["gait-assist"], 10)
    assert blocked.value.context["reason"] == "site_capacity_full"

    clock.advance(seconds=11)
    outcome = service.recover_expired()
    assert outcome["cancelled"] == [held["id"]]
    claimed = service.claim("site-a", ["gait-assist"], 60)
    assert claimed["session"]["id"] == next_one["id"]


def test_concurrent_claims_never_oversubscribe_single_capacity(client):
    import threading

    create_protocol(client)
    register_site("single", ["gait-assist"], max_concurrent=1)
    for index in range(5):
        client.post("/api/pilots/sessions", json=submit_payload(f"parallel-{index}"))

    results: list[dict] = []
    barrier = threading.Barrier(5)

    def worker() -> None:
        from app.database import get_connection as thread_connection

        service = PilotOperationsService(thread_connection())
        barrier.wait()
        try:
            outcome = service.claim("single", ["gait-assist"], 60)
            results.append({"ok": outcome["session"] is not None})
        except ConflictError as exc:
            results.append({"ok": False, "reason": exc.context["reason"]})

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 5
    assert sum(1 for item in results if item["ok"]) == 1
    assert all(item["ok"] or item["reason"] == "site_capacity_full" for item in results)
    running = client.get("/api/pilots/sessions?status=running").json()["items"]
    assert len(running) == 1
    assert len(client.get("/api/pilots/sessions?status=queued").json()["items"]) == 4


def test_lease_recovery_frees_capacity_immediately(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 4, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    register_site("site-a", ["gait-assist"], max_concurrent=1, clock=clock)
    held = service.submit(submit_payload("recover-held"))
    other = service.submit(submit_payload("recover-other"))
    assert service.claim("site-a", ["gait-assist"], 10)["session"]["id"] == held["id"]

    # 租约过期第一次重新排队（仍占用被释放），第二次耗尽转失败。
    clock.advance(seconds=11)
    assert service.recover_expired()["recovered"] == [held["id"]]
    assert service.claim("site-a", ["gait-assist"], 10)["session"]["id"] == held["id"]
    clock.advance(seconds=11)
    assert service.recover_expired()["exhausted"] == [held["id"]]

    # 恢复后名额立即释放，排队中的另一场可被领取。
    claimed = service.claim("site-a", ["gait-assist"], 60)
    assert claimed["session"]["id"] == other["id"]
    assert claimed["site"]["active_sessions"] == 1
    assert claimed["site"]["available_slots"] == 0

