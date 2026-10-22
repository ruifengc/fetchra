#!/usr/bin/env python3
"""Package the binary, actual dependency notices, and matching source archives.

Only modules contributing packages to the executable are included. Unmodified
dependency source is shipped, rather than relying on a mutable upstream link.
"""
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    go = os.environ.get("GO", "go")
    output = subprocess.check_output([go, "list", "-tags", "nosqlite", "-deps", "-json", "./cmd/fetchra"], cwd=ROOT, text=True)
    decoder = json.JSONDecoder()
    modules = {}
    while output.strip():
        package, end = decoder.raw_decode(output.lstrip())
        output = output.lstrip()[end:]
        module = package.get("Module", {})
        if module and not module.get("Main"):
            modules[module["Path"]] = module
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    platform = subprocess.check_output([go, "env", "GOOS", "GOARCH"], cwd=ROOT, text=True).split()
    version = re.search(r'const Version = "([0-9]+\.[0-9]+\.[0-9]+)"', (ROOT / "internal/model/model.go").read_text()).group(1)
    name = "fetchra-" + version + "-" + "-".join(platform)
    with tempfile.TemporaryDirectory(prefix="fetchra-release-") as temporary:
        stage = Path(temporary) / name
        stage.mkdir()
        for source in (ROOT / "bin/fetchra", ROOT / "README.md", ROOT / "LICENSE"):
            shutil.copy2(source, stage / source.name)
        shutil.copytree(ROOT / "docs", stage / "docs")
        notices = stage / "licenses"
        notices.mkdir()
        manifest = ["# Third-party software", "", "Exact matching dependency source is included in dependency-sources.tar.gz.", "", "Original Fetchra code is MIT; dependency licenses remain applicable.", ""]
        go_root = Path(subprocess.check_output([go, "env", "GOROOT"], cwd=ROOT, text=True).strip())
        go_version = subprocess.check_output([go, "version"], cwd=ROOT, text=True).strip()
        go_notices = notices / "go"
        go_notices.mkdir()
        shutil.copy2(go_root / "LICENSE", go_notices / "LICENSE")
        for filename in ("PATENTS", "NOTICE"):
            if (go_root / filename).is_file():
                shutil.copy2(go_root / filename, go_notices / filename)
        manifest.append(f"- Go runtime and standard library ({go_version}): notices in licenses/go")
        with tarfile.open(stage / "dependency-sources.tar.gz", "w:gz") as archive:
            for module_path, module in sorted(modules.items()):
                source = Path(module["Dir"])
                version = module["Version"]
                module_name = module_path.replace("/", "_") + "@" + version
                archive.add(source, arcname=module_name)
                directory = notices / module_name
                directory.mkdir()
                found = False
                for item in source.rglob("*"):
                    if item.is_file() and item.name.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE", "PATENTS")):
                        relative = item.relative_to(source)
                        target = directory / relative
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(item, target)
                        found = True
                if not found:
                    raise RuntimeError("Missing license for dependency: " + module_path)
                manifest.append(f"- {module_path} {version}: notices in licenses/{module_name}; source in dependency-sources.tar.gz")
        (stage / "THIRD_PARTY.md").write_text("\n".join(manifest) + "\n", encoding="utf-8")
        artifact = dist / (name + ".tar.gz")
        with tarfile.open(artifact, "w:gz") as archive:
            archive.add(stage, arcname=name)
        print(artifact)


if __name__ == "__main__":
    main()
