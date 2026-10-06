#!/usr/bin/env python3
"""Give management error responses JSON media types before OpenAPI conversion."""

import json
import sys
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit("Usage: openapi-json-errors.py INPUT.json OUTPUT.json")

source, target = map(Path, sys.argv[1:])
document = json.loads(source.read_text(encoding="utf-8"))

# Document-level responses use the document's produces list instead of a binary endpoint's list
document["produces"] = ["application/json"]
for path in document["paths"].values():
    for method in ("get", "put", "post", "delete", "options", "head", "patch"):
        operation = path.get(method)
        if operation is None or operation.get("produces", document["produces"]) == ["application/json"]:
            continue

        # Share error responses so the converter keeps JSON errors and the operation's success media type
        for status, response in operation.get("responses", {}).items():
            if not status.startswith(("4", "5")) or "$ref" in response:
                continue
            name = f'{operation["operationId"]}_{status}'
            shared = document.setdefault("responses", {})
            if name in shared:
                raise ValueError(f"Duplicate response name: {name}")
            shared[name] = response
            operation["responses"][status] = {"$ref": f"#/responses/{name}"}

target.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
