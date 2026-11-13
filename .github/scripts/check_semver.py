"""Check that the package version follows Semantic Versioning 2.0.0.

Verifies that the version in pyproject.toml is a valid SemVer string, that it
matches the docs ``release`` in docs/conf.py, and (when a tag is given) that the
tag is ``v<version>``. Usage: check_semver.py [TAG]
"""
import re
import sys

# Official regex from https://semver.org/
SEMVER = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)


def find(path, pattern):
    with open(path) as f:
        match = re.search(pattern, f.read(), re.MULTILINE)
    if not match:
        sys.exit(f"could not find version in {path}")
    return match.group(1)


def main(tag=None):
    version = find("pyproject.toml", r'^version\s*=\s*"([^"]+)"')
    errors = []
    if not SEMVER.match(version):
        errors.append(f"pyproject.toml version {version!r} is not valid SemVer 2.0.0")
    release = find("docs/conf.py", r'^release\s*=\s*"([^"]+)"')
    if release != version:
        errors.append(f"docs/conf.py release {release!r} != pyproject.toml version {version!r}")
    if tag and tag != f"v{version}":
        errors.append(f"tag {tag!r} != 'v{version}'")
    if errors:
        sys.exit("\n".join(errors))
    print(f"OK: version {version} follows SemVer 2.0.0")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
