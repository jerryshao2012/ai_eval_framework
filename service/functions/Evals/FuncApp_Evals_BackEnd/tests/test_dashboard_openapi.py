from dashboard.app import app


def test_openapi_and_docs_endpoints() -> None:
    client = app.test_client()

    openapi_resp = client.get("/api/openapi.json")
    assert openapi_resp.status_code == 200
    spec = openapi_resp.get_json()
    assert spec["openapi"].startswith("3.")
    assert "/api/latest" in spec["paths"]

    docs_resp = client.get("/api/docs")
    assert docs_resp.status_code == 200
    assert "swagger-ui" in docs_resp.get_data(as_text=True).lower()
