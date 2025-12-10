import json
import random
from jsonschema import validate

# ---- Load main schema ----
with open("validation/WorkZoneFeed.json") as f:
    schema = json.load(f)
# ---- Load data ----
with open("wzdx_output.geojson") as f:
    data = json.load(f)

# ---- Sampling Logic ----
features = data.get("features", [])
sample_size = 25  # adjust if desired

if sample_size > len(features):
    raise ValueError(f"Sample size {sample_size} exceeds total features {len(features)}")

sampled = random.sample(features, sample_size)
data["features"] = sampled

print(f"🔍 Validating a random sample of {sample_size} feature(s) out of {len(features)} total\n")

# ---- Validate ----
validate(instance=data, schema=schema)

print("\n✅ data sample passes schema validation")
