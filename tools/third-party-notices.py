"""Writes the licence notices of the libraries that are built into rexyra-server.

Usage: python tools/third-party-notices.py OUTPUT_FILE

Rexyra itself is in the public domain, but the program contains other
people's code, and their licences ask for their notices to accompany it.
This collects them: every crate the server links on Windows, from
`cargo metadata`, with the licence files each one ships, and the Lua
interpreter, which is compiled in from the `lua-src` crate.

Run by installer/build.ps1 and by the release workflow, so the file in a
release always matches what was built.
"""
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
TARGET = "x86_64-pc-windows-msvc"
SERVER = "rexyra-server"
# Crates that are only build dependencies but whose contents end up in the
# program: lua-src carries the source of Lua itself.
ALSO_LINKED = {"lua-src"}
LICENCE_FILES = ("license", "licence", "copying", "notice", "unlicense", "copyright")


def metadata():
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--filter-platform", TARGET, "--locked"],
        cwd=ROOT, check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout
    return json.loads(out)


def linked_packages(meta):
    """Every package reachable from the server through normal dependencies."""
    packages = {p["id"]: p for p in meta["packages"]}
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    workspace = set(meta["workspace_members"])
    start = next(i for i in workspace if packages[i]["name"] == SERVER)
    seen, queue = {start}, [start]
    while queue:
        for dep in nodes[queue.pop()]["deps"]:
            normal = any(kind["kind"] is None for kind in dep["dep_kinds"])
            also = packages[dep["pkg"]]["name"] in ALSO_LINKED
            if (normal or also) and dep["pkg"] not in seen:
                seen.add(dep["pkg"])
                queue.append(dep["pkg"])
    return sorted(
        (packages[i] for i in seen if i not in workspace),
        key=lambda p: (p["name"].lower(), p["version"]),
    )


def licence_texts(package):
    folder = pathlib.Path(package["manifest_path"]).parent
    found = []
    for path in sorted(folder.iterdir(), key=lambda p: p.name.lower()):
        if path.is_file() and path.name.lower().startswith(LICENCE_FILES):
            found.append((path.name, path.read_text(encoding="utf-8", errors="replace").strip()))
    return found


def lua_copyright(package):
    """Lua's own notice, which sits inside the crate that carries its source."""
    folder = pathlib.Path(package["manifest_path"]).parent
    # It is the comment that ends lua.h, of the version the server uses.
    for header in sorted(folder.glob("lua-5.1*/lua.h")):
        lines = header.read_text(encoding="utf-8", errors="replace").splitlines()
        starts = [i for i, line in enumerate(lines) if "Copyright (C)" in line and "All rights reserved" in line]
        if not starts:
            continue
        notice = []
        for line in lines[starts[-1]:]:
            if "*/" in line:
                break
            notice.append(line.lstrip("*").strip())
        return "\n".join(notice).strip()
    return None


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    packages = linked_packages(metadata())
    rule = "=" * 78
    out = [
        "THIRD-PARTY NOTICES",
        "",
        "Rexyra is released into the public domain (see LICENSE). The program",
        "rexyra-server.exe also contains the libraries listed below, which remain",
        "under their own licences. Their notices follow.",
        "",
    ]
    missing = []
    for package in packages:
        out += [rule, f"{package['name']} {package['version']}"]
        out += [f"Licence: {package.get('license') or 'see below'}"]
        if package.get("repository"):
            out += [f"Source:  {package['repository']}"]
        out += [rule, ""]
        texts = licence_texts(package)
        if not texts:
            missing.append(package["name"])
            out += ["(This crate ships no licence file; its licence is the one named above.)", ""]
        for name, text in texts:
            out += [f"--- {name} ---", "", text, ""]
        if package["name"] == "lua-src":
            lua = lua_copyright(package)
            if lua is None:
                raise SystemExit("the Lua 5.1 COPYRIGHT file was not found in lua-src")
            out += ["--- Lua 5.1, the interpreter itself (from lua.h) ---", "", lua, ""]
    pathlib.Path(sys.argv[1]).write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {sys.argv[1]}: {len(packages)} libraries" + (f"; without a licence file: {', '.join(missing)}" if missing else ""))


if __name__ == "__main__":
    main()
