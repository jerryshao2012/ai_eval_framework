from dashboard.app import app


def test_dashboard_alerts_use_default_thresholds() -> None:
    client = app.test_client()
    response = client.get("/api/latest")
    assert response.status_code == 200
    rows = response.get_json()

    app1 = next(row for row in rows if row["app_id"] == "app1")
    assert app1["status"] == "warning"
    assert len(app1["breaches"]) == 2


def test_dashboard_dynamic_thresholds_override_defaults() -> None:
    client = app.test_client()
    response = client.get(
        "/api/latest?dynamic_thresholds=1"
        "&threshold.accuracy.warning=0.90&direction.accuracy.warning=min"
        "&threshold.latency_p95_ms.warning=1500&direction.latency_p95_ms.warning=max"
    )
    assert response.status_code == 200
    rows = response.get_json()

    app1 = next(row for row in rows if row["app_id"] == "app1")
    assert app1["status"] == "healthy"
    assert app1["breaches"] == []
