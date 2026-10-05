from __future__ import annotations

from datetime import UTC, datetime

from app.pilots.service import PilotOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, transaction


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


def site_payload(code: str, capabilities: list[str], max_concurrent: int = 1) -> dict:
    return {
        "code": code,
        "name": f"体验点-{code}",
        "site_type": "展会体验点",
        "region": "杭州",
        "capabilities": capabilities,
        "max_concurrent": max_concurrent,
    }


def create_site(client, code: str, capabilities: list[str], max_concurrent: int = 1) -> None:
    response = client.post("/api/catalog/sites", json=site_payload(code, capabilities, max_concurrent))
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
    create_site(client, "w0", ["other"])
    create_site(client, "w1", ["gait-assist"])
    low = client.post("/api/pilots/sessions", json=submit_payload("priority-low", priority=10)).json()
    high = client.post("/api/pilots/sessions", json=submit_payload("priority-high", priority=90)).json()
    no_match = client.post("/api/pilots/sessions/claim", json={"site_code": "w0", "capabilities": ["other"], "lease_seconds": 60})
    assert no_match.status_code == 200 and no_match.json()["session"] is None
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
    from app.catalog.service import CatalogService
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    connection = get_connection()
    service = PilotOperationsService(connection, clock)
    CatalogService(connection, clock).create_site(site_payload("site-a", ["gait-assist"]))
    service.create_protocol(PROTOCOL, "administrator")
    first = service.submit(submit_payload("failure-000001"))
    claimed = service.claim("site-a", ["gait-assist"], 10)
    assert claimed and claimed["id"] == first["id"]
    failed = service.fail(first["id"], "site-a", "sensor_unstable", "步态传感器读数不稳定", True)
    assert failed["status"] == "queued"
    assert failed["available_at"] > failed["updated_at"]
    clock.advance(seconds=2)
    claimed_again = service.claim("site-a", ["gait-assist"], 10)
    assert claimed_again and claimed_again["attempt_count"] == 2
    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["exhausted"] == [first["id"]]
    details = service.get_session(first["id"])
    assert details["status"] == "failed"
    assert details["interventions"][-1]["action"] == "lease_recovery"


def test_claim_uses_registered_site_capabilities(client):
    create_protocol(client)
    create_site(client, "expo-other", ["other"])
    client.post("/api/pilots/sessions", json=submit_payload("capability-000001"))
    # 场地不能领取登记能力之外的场次，即使请求里自称具备该能力
    claimed = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-other", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["session"] is None
    queued = client.get("/api/pilots/sessions?status=queued").json()["items"]
    assert len(queued) == 1


def test_claim_enforces_site_status_and_capacity(client):
    create_protocol(client)
    create_site(client, "expo-single", ["gait-assist"])
    first = client.post("/api/pilots/sessions", json=submit_payload("capacity-000001")).json()
    second = client.post("/api/pilots/sessions", json=submit_payload("capacity-000002")).json()

    unknown = client.post("/api/pilots/sessions/claim", json={"site_code": "missing-site", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert unknown.status_code == 404

    claimed = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["session"]["id"] == first["id"]

    # 单容量场地持有运行场次时，第二个领取请求被拒绝并说明原因
    blocked = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert blocked.status_code == 409
    assert blocked.json()["error"]["context"]["reason"] == "site_at_capacity"
    assert blocked.json()["error"]["context"]["active_sessions"] == 1

    # 被拒绝的请求没有拿走队列项，占用情况可以实时查看
    occupancy = client.get("/api/pilots/sites/expo-single/occupancy")
    assert occupancy.status_code == 200
    assert occupancy.json()["active_sessions"] == 1
    assert occupancy.json()["available_slots"] == 0
    assert [item["id"] for item in occupancy.json()["occupying_sessions"]] == [first["id"]]
    queued = client.get("/api/pilots/sessions?status=queued").json()["items"]
    assert [item["id"] for item in queued] == [second["id"]]

    # 暂停或关闭的场地不能领单
    assert client.patch("/api/catalog/sites/expo-single", json={"status": "suspended"}).status_code == 200
    paused = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert paused.status_code == 409
    assert paused.json()["error"]["context"]["reason"] == "site_suspended"
    assert client.patch("/api/catalog/sites/expo-single", json={"status": "closed"}).status_code == 200
    closed = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert closed.status_code == 409
    assert closed.json()["error"]["context"]["reason"] == "site_closed"

    # 场地恢复且场次完成后，名额立即按真实状态释放
    assert client.patch("/api/catalog/sites/expo-single", json={"status": "active"}).status_code == 200
    completed = client.post(f"/api/pilots/sessions/{first['id']}/complete", json={"site_code": "expo-single", "observation": {"ok": True}, "metrics": {}})
    assert completed.status_code == 200
    assert client.get("/api/pilots/sites/expo-single/occupancy").json()["available_slots"] == 1
    again = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert again.status_code == 200 and again.json()["session"]["id"] == second["id"]


def test_cancel_requested_session_still_occupies_capacity(client):
    create_protocol(client)
    create_site(client, "expo-single", ["gait-assist"])
    running = client.post("/api/pilots/sessions", json=submit_payload("cancel-hold-001")).json()
    client.post("/api/pilots/sessions", json=submit_payload("cancel-hold-002"))
    claimed = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert claimed.json()["session"]["id"] == running["id"]
    cancelled = client.post(f"/api/pilots/sessions/{running['id']}/cancel", json={"actor": "administrator", "reason": "设备安全检查"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancel_requested"
    # 等待安全停止的场次仍然占用容量
    blocked = client.post("/api/pilots/sessions/claim", json={"site_code": "expo-single", "capabilities": ["gait-assist"], "lease_seconds": 60})
    assert blocked.status_code == 409
    occupancy = client.get("/api/pilots/sites/expo-single/occupancy").json()
    assert occupancy["active_sessions"] == 1
    assert occupancy["occupying_sessions"][0]["status"] == "cancel_requested"


def test_capacity_recomputed_after_failure_recovery_and_expansion(client):
    from app.catalog.service import CatalogService
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=UTC))
    connection = get_connection()
    service = PilotOperationsService(connection, clock)
    catalog = CatalogService(connection, clock)
    catalog.create_site(site_payload("expo-single", ["gait-assist"]))
    service.create_protocol(PROTOCOL, "administrator")
    first = service.submit(submit_payload("recompute-000001"))
    second = service.submit(submit_payload("recompute-000002"))

    claimed = service.claim("expo-single", ["gait-assist"], 30)
    assert claimed and claimed["id"] == first["id"]
    # 不可重试的失败立即释放名额
    failed = service.fail(first["id"], "expo-single", "sensor_fault", "传感器硬件故障", False)
    assert failed["status"] == "failed"
    occupancy = service.site_occupancy("expo-single")
    assert occupancy["active_sessions"] == 0 and occupancy["available_slots"] == 1

    # 租约恢复后名额立即释放
    claimed_second = service.claim("expo-single", ["gait-assist"], 30)
    assert claimed_second and claimed_second["id"] == second["id"]
    clock.advance(seconds=31)
    assert service.recover_expired()["recovered"] == [second["id"]]
    assert service.site_occupancy("expo-single")["active_sessions"] == 0

    # 场地扩容后可用名额立即增加
    third = service.submit(submit_payload("recompute-000003"))
    running = service.claim("expo-single", ["gait-assist"], 30)
    assert running and running["id"] == second["id"]
    catalog.update_site("expo-single", {"max_concurrent": 2})
    expanded = service.claim("expo-single", ["gait-assist"], 30)
    assert expanded and expanded["id"] == third["id"]
    occupancy = service.site_occupancy("expo-single")
    assert occupancy["active_sessions"] == 2 and occupancy["available_slots"] == 0


def test_concurrent_claims_never_exceed_site_capacity(client):
    import threading

    from app.catalog.service import CatalogService
    from app.core.errors import ConflictError
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 9, 0, tzinfo=UTC))
    connection = get_connection()
    service = PilotOperationsService(connection, clock)
    catalog = CatalogService(connection, clock)

    for site_code, capacity in (("expo-race-1", 1), ("expo-race-2", 2)):
        capability = f"gait-{site_code}"
        service.create_protocol({**PROTOCOL, "code": capability, "capability": capability}, "administrator")
        catalog.create_site(site_payload(site_code, [capability], capacity))
        submitted = [
            service.submit({**submit_payload(f"race-{site_code}-{index:06d}"), "protocol_code": capability})["id"]
            for index in range(capacity + 3)
        ]
        won: list[dict] = []
        rejected: list[Exception] = []
        lock = threading.Lock()

        def attempt() -> None:
            local = PilotOperationsService(clock=clock)
            try:
                value = local.claim(site_code, [capability], 60)
                with lock:
                    if value is not None:
                        won.append(value)
            except ConflictError as exc:
                with lock:
                    rejected.append(exc)

        threads = [threading.Thread(target=attempt) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # 同时到达的领取请求不会超出并发上限，失败请求不占用队列项
        assert len(won) == capacity
        assert len({item["id"] for item in won}) == capacity
        assert len(rejected) == 8 - capacity
        occupancy = service.site_occupancy(site_code)
        assert occupancy["active_sessions"] == capacity
        assert occupancy["available_slots"] == 0
        queued_ids = {row["id"] for row in service.list_sessions(status="queued")}
        assert {session_id for session_id in submitted if session_id in queued_ids} == set(submitted) - {item["id"] for item in won}
        assert len({session_id for session_id in submitted if session_id in queued_ids}) == 3

