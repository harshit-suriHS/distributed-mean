#!/usr/bin/env sh
# Regenerates the gRPC code from rpc/task_queue.proto (run from worker/).
set -eu
uv run python -m grpc_tools.protoc -I src \
  --python_out=src --grpc_python_out=src --pyi_out=src \
  src/dmean_worker/rpc/task_queue.proto
