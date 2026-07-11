#!/usr/bin/env bash
# Regenerate Python gRPC stubs from proto/*.proto.
#
# Usage:
#   ./scripts/regenerate-proto.sh
#
# Requires grpcio-tools (installed via `pip install -e .[dev]`).
# Output:
#   src/sutradhara/_proto/*_pb2.py + *_pb2_grpc.py
#
# The generated files are committed; do not edit them by hand.

set -euo pipefail

cd "$(dirname "$0")/.."

PROTO_SRC="proto"
SERVER_PROTO_OUT="src/sutradhara/_proto"

PYTHON_CHECK="${PYTHON:-python3}"
if [[ -x .venv/bin/python ]]; then
    PYTHON_CHECK=.venv/bin/python
fi
if ! "${PYTHON_CHECK}" -c "import grpc_tools" 2>/dev/null; then
    echo "error: grpcio-tools not installed in ${PYTHON_CHECK}. Run:" >&2
    echo "  pip install -e '.[dev]'" >&2
    exit 1
fi

mkdir -p "${SERVER_PROTO_OUT}"

# protoc options:
#   --python_out      → message classes (*_pb2.py)
#   --grpc_python_out → stubs (*_pb2_grpc.py)
#   --proto_path      → where to resolve imports (.proto file source roots)
# Use the venv's grpcio-tools (newer protoc) if a .venv is present.
# Falls back to system python3 if not.
PYTHON="${PYTHON:-python3}"
if [[ -x .venv/bin/python ]]; then
    PYTHON=.venv/bin/python
fi

"${PYTHON}" -m grpc_tools.protoc \
    --proto_path="${PROTO_SRC}" \
    --python_out="${SERVER_PROTO_OUT}" \
    --pyi_out="${SERVER_PROTO_OUT}" \
    --grpc_python_out="${SERVER_PROTO_OUT}" \
    "${PROTO_SRC}/layer5.proto" \
    "${PROTO_SRC}/intake.proto" \
    "${PROTO_SRC}/device.proto" \
    "${PROTO_SRC}/restore.proto"

# protoc emits imports like `import layer5_pb2`, which only works if
# ${PROTO_OUT} is on sys.path. We're a package, so rewrite to a relative
# import. (Standard workaround; see grpc/grpc#9575.)
python3 - <<'PY'
import pathlib
import re

out = pathlib.Path("src/sutradhara/_proto")
for grpc_file in out.glob("*_pb2_grpc.py"):
    text = grpc_file.read_text()
    text = re.sub(
        r"^import (\w+_pb2) as",
        r"from . import \1 as",
        text,
        flags=re.MULTILINE,
    )
    grpc_file.write_text(text)
PY

touch "${SERVER_PROTO_OUT}/__init__.py"

echo "generated: ${SERVER_PROTO_OUT}/*_pb2.py"
echo "generated: ${SERVER_PROTO_OUT}/*_pb2.pyi"
echo "generated: ${SERVER_PROTO_OUT}/*_pb2_grpc.py"
