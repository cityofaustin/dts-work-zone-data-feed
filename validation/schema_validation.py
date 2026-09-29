import json
import sys
import time
import jsonschema_rs

MAX_ERRORS_SHOWN = 10

# ---- Load schema + data ----
with open("validation/WorkZoneFeed_schema.json") as f:
    schema = json.load(f)
with open("wzdx_output.geojson") as f:
    data = json.load(f)

features = data.get("features", [])
print(f"🔍 Validating all {len(features)} feature(s)\n")

# ---- Build validator once (picks the draft from the schema's $schema) ----
validator = jsonschema_rs.validator_for(schema)

# ---- Validate ----
start = time.perf_counter()
errors = list(validator.iter_errors(data))
elapsed = time.perf_counter() - start

if not errors:
    print(f"✅ data passes schema validation ({elapsed:.2f}s)")
    sys.exit(0)

print(f"❌ {len(errors)} validation error(s) found ({elapsed:.2f}s)\n")
for e in errors[:MAX_ERRORS_SHOWN]:
    path = "/".join(str(p) for p in e.instance_path)
    print(f"- at {path or '<root>'}: {e.message[:200]}")
if len(errors) > MAX_ERRORS_SHOWN:
    print(f"... and {len(errors) - MAX_ERRORS_SHOWN} more")
sys.exit(1)
