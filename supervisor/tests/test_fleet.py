import json
import sys
import time
from pathlib import Path

import pytest

from dmean_supervisor.fleet import Fleet


def test_workers_do_not_inherit_database_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "SUPERVISOR_DB_PASSWORD",
        "API_DB_PASSWORD",
        "ORCHESTRATOR_DB_PASSWORD",
    ]:
        monkeypatch.setenv(name, "secret")
    monkeypatch.setenv("S3_SECRET_KEY", "s3-secret")
    monkeypatch.setenv("ORCHESTRATOR_ADDR", "orchestrator:50051")
    out = tmp_path / "env.json"
    dump = f"import json, os; open({str(out)!r}, 'w').write(json.dumps(dict(os.environ)))"

    fleet = Fleet([sys.executable, "-c", dump], "test")
    worker_id = fleet.spawn()
    deadline = time.monotonic() + 10
    while not fleet.reap() and time.monotonic() < deadline:
        time.sleep(0.05)

    env = json.loads(out.read_text())
    assert not [k for k in env if k.startswith("POSTGRES_") or k.endswith("_DB_PASSWORD")]
    assert env["WORKER_ID"] == worker_id
    assert env["S3_SECRET_KEY"] == "s3-secret"
    assert env["ORCHESTRATOR_ADDR"] == "orchestrator:50051"
    assert "PATH" in env
