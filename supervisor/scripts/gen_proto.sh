#!/usr/bin/env sh
# Regenerates the gRPC code from rpc/*.proto (run from supervisor/).
set -eu
uv run python -m grpc_tools.protoc -I src \
  --python_out=src --grpc_python_out=src --pyi_out=src \
  src/dmean_supervisor/rpc/admin.proto
