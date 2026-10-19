"""Storage keys for outputs. Deterministic, so a retried task writes to the same place."""


def partial_key(job_uuid: str, task_id: int) -> str:
    return f"partials/{job_uuid}/{task_id}.csv"


def result_key(job_uuid: str) -> str:
    return f"results/{job_uuid}.csv"
