import tomlkit
from pathlib import Path

target_path = Path("Cargo.toml")
source_path = Path("../Cargo.toml")

# Read the file
target_doc = tomlkit.parse(target_path.read_text())
source_doc = tomlkit.parse(source_path.read_text())

# Get from source and apply to target
for dep_name in ["zenoh", "zenoh-config"]:
    source_dep = source_doc["workspace"]["dependencies"][dep_name]
    target_doc["dependencies"][dep_name] = source_dep

# Write changes back to target
print(target_doc)
target_path.write_text(tomlkit.dumps(target_doc))
