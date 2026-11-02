"""Write docs/message-schema.json from the code (tests/test_message_schema.py fails until the file matches).

.venv/bin/python scripts/export_message_schema.py
"""

from __future__ import annotations

from pathlib import Path

from orderly.messages import json_schema_text

TARGET = Path(__file__).parents[1] / "docs" / "message-schema.json"

if __name__ == "__main__":
    TARGET.write_text(json_schema_text())
    print(f"wrote {TARGET}")
