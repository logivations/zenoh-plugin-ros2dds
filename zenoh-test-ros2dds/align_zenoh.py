import json
import subprocess
from pathlib import Path

import tomlkit

target_path = Path("Cargo.toml")
source_path = Path("../Cargo.toml")

# Read the file
target_doc = tomlkit.parse(target_path.read_text())
source_doc = tomlkit.parse(source_path.read_text())

# Get from source and apply to target
for dep_name in ["zenoh", "zenoh-config"]:
    source_dep = source_doc["workspace"]["dependencies"][dep_name]
    target_doc["dependencies"][dep_name] = source_dep

# This is an independent workspace: Cargo does not inherit its parent's patches.
# Keep the release manifest authoritative, including removal of obsolete patches.
target_doc.pop("patch", None)
if "patch" in source_doc:
    target_doc["patch"] = source_doc["patch"]

# Write changes back to target
print(target_doc)
target_path.write_text(tomlkit.dumps(target_doc))

# Resolve before compiling, then guard the actual sources (not only the requested
# versions). Cargo otherwise silently accepts an unused version-mismatched patch.
metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--format-version=1"]))
patches = source_doc.get("patch", {}).get("crates-io", {})
for package in metadata["packages"]:
    patch = patches.get(package["name"], {})
    if "git" in patch and "rev" in patch:
        expected = f"git+{patch['git']}?rev={patch['rev']}#{patch['rev']}"
        if package["source"] != expected:
            raise RuntimeError(f"{package['name']} resolved to {package['source']}, expected {expected}")
