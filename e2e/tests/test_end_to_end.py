"""The whole system, as a user sees it: upload, submit, poll, read the mean."""

import time

import numpy as np
from conftest import Client, Proc, start_orchestrator, start_worker, unique_name, wait_until


def data(files: int, columns: int, seed: int) -> np.ndarray:
    return np.random.default_rng(seed).random((files, columns))


def test_problem_statement_example(orchestrator: Proc, workers: list[Proc], client: Client) -> None:
    workers.append(start_worker(unique_name("w")))
    job = client.submit([[1, 2, 3], [4, 5, 6]])

    status = client.wait(job)

    assert status["status"] == "succeeded", status
    assert client.result(job) == [2.5, 3.5, 4.5]


def test_mean_matches_numpy_through_maps_and_merges(
    orchestrator: Proc, workers: list[Proc], client: Client
) -> None:
    workers += [start_worker(unique_name("w")) for _ in range(3)]
    rows = data(files=20, columns=200, seed=1)  # 3 files per map → 7 maps, then merges

    job = client.submit(rows.tolist())
    status = client.wait(job)

    assert status["status"] == "succeeded", status
    assert status["progress"] == {"files_done": 20, "percent": 100}
    np.testing.assert_allclose(client.result(job), rows.mean(axis=0), rtol=1e-13)
    assert status["tasks"]["done"] > 7  # maps and merges


def test_several_jobs_at_once(orchestrator: Proc, workers: list[Proc], client: Client) -> None:
    workers += [start_worker(unique_name("w")) for _ in range(3)]
    sets = [data(files=n, columns=50, seed=n) for n in (9, 14, 5)]

    jobs = [client.submit(rows.tolist()) for rows in sets]

    for job, rows in zip(jobs, sets, strict=True):
        assert client.wait(job)["status"] == "succeeded"
        np.testing.assert_allclose(client.result(job), rows.mean(axis=0), rtol=1e-13)


def test_bad_input_fails_the_job_with_the_reason(
    orchestrator: Proc, workers: list[Proc], client: Client
) -> None:
    workers.append(start_worker(unique_name("w")))
    rows = data(files=4, columns=3, seed=2).tolist()
    rows[2] = rows[2][:2]  # one file has 2 values instead of 3

    status = client.wait(client.submit(rows))

    assert status["status"] == "failed"
    assert status["failure_type"] == "user_input"
    assert "expected 3 values, got 2" in status["error"]


def test_killed_worker_mid_job_still_gives_the_right_mean(
    orchestrator: Proc, workers: list[Proc], client: Client
) -> None:
    victim = start_worker(unique_name("victim"), slowdown=1.0)  # 1 s per file: easy to catch
    workers.append(victim)
    rows = data(files=12, columns=40, seed=3)
    job = client.submit(rows.tolist())
    wait_until(lambda: client.status(job)["tasks"]["running"] >= 1, 20, "a running task")

    victim.kill()  # SIGKILL mid-task: no report, no goodbye
    workers.append(start_worker(unique_name("rescuer")))

    status = client.wait(job, timeout=90)
    assert status["status"] == "succeeded", status
    np.testing.assert_allclose(client.result(job), rows.mean(axis=0), rtol=1e-13)


def test_orchestrator_restart_mid_job(
    orchestrator: Proc, workers: list[Proc], client: Client
) -> None:
    workers += [start_worker(unique_name("w"), slowdown=0.3) for _ in range(2)]
    rows = data(files=15, columns=40, seed=4)
    job = client.submit(rows.tolist())
    wait_until(lambda: client.status(job)["tasks"]["done"] >= 1, 30, "first finished task")

    assert orchestrator.stop() == 0  # SIGTERM: a clean stop mid-job
    time.sleep(1)
    replacement = start_orchestrator("orchestrator-2")  # rebuilds from Postgres
    try:
        status = client.wait(job, timeout=90)
    finally:
        assert replacement.stop() == 0
    assert status["status"] == "succeeded", status
    np.testing.assert_allclose(client.result(job), rows.mean(axis=0), rtol=1e-13)


def test_slow_worker_gets_fewer_tasks(
    orchestrator: Proc, workers: list[Proc], client: Client
) -> None:
    slow, fast = unique_name("slow"), unique_name("fast")
    workers += [start_worker(slow, slowdown=0.6), start_worker(fast)]
    rows = data(files=30, columns=20, seed=5)

    job = client.submit(rows.tolist())
    assert client.wait(job, timeout=120)["status"] == "succeeded"

    by_worker = client.tasks_by_worker(job)
    assert by_worker.get(fast, 0) > by_worker.get(slow, 0), by_worker
