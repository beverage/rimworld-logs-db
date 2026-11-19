"""Read the mod list, game version and Harmony patches off a HugsLib log.

A shared log opens with two blocks HugsLib writes itself, before the game's
own log:

    Loaded mods:
    Harmony(brrainz.harmony)[mv:2.3.1.0]: 0Harmony(2.3.3), HarmonyMod(2.3.1)
    Core(Ludeon.RimWorld): (no assemblies)

    Active Harmony patches:
    Pawn.Tick: PRE: [800]Some.Mod.Patch.Prefix, Other.Mod.Patch.Prefix post: Third.Mod.Patch.Postfix

Each block ends at the first blank line. These functions are pure: text in,
facts out.
"""

from __future__ import annotations

import hashlib
import re

# "Name(packageId)[mv:1.2]: assemblies". The name may hold parentheses of its
# own, so the packageId is the last parenthesised token before the colon.
MOD_LINE = re.compile(r"^(?P<name>.*)\((?P<package>[^()\s]+)\)(?:\[[^\]]*\])?: ")
BLOCK_END = re.compile(r"\r?\n[ \t]*\r?\n")
GAME_VERSION = re.compile(r"RimWorld (\d+\.\d+\.\d+) rev(\d+)")
DLC_PREFIX = "ludeon.rimworld."
TRIM_MARK = "(log trimmed to"

PATCH_SECTION = re.compile(r"(?:^| )(PRE|post|TRANS): ")
PATCH_PRIORITY = re.compile(r"^\[(-?\d+)\]")
PATCH_KINDS = {"PRE": "prefix", "post": "postfix", "TRANS": "transpiler"}

# A patch is identified by these four printed parts, in this order.
Patch = tuple[str, str, int | None, str]  # target, kind, priority, method


def decode(raw: bytes) -> tuple[str, bool]:
    """A log's text, and whether it was valid UTF-8. Invalid bytes are replaced."""
    try:
        return raw.decode("utf-8"), True
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace"), False


def block_after(text: str, heading: str) -> list[str]:
    """The lines under a heading, up to the first blank line. Empty if the heading is absent."""
    start = text.find(heading)
    if start == -1:
        return []
    return BLOCK_END.split(text[start:], maxsplit=1)[0].splitlines()[1:]


def parse_log(text: str) -> dict:
    """Facts read off a HugsLib log: its mod list, DLC, game version and size."""
    mods = [match["package"] for line in block_after(text, "Loaded mods:") if (match := MOD_LINE.match(line))]
    version = GAME_VERSION.search(text)
    return {
        "mods": mods,
        "mod_count": len(mods),
        "dlcs": sorted(m[len(DLC_PREFIX) :] for m in mods if m.lower().startswith(DLC_PREFIX)),
        "game_version": version[1] if version else None,
        "game_rev": int(version[2]) if version else None,
        "trimmed": TRIM_MARK in text,
        "line_count": text.count("\n") + 1,
    }


def parse_patches(text: str) -> list[Patch]:
    """The distinct patches in the Harmony patch block.

    HugsLib prints one line per patched method, "Type.Method: PRE: a, b post: c
    TRANS: d", naming each patch by its full method name with "[priority]" in
    front when the priority is not the default. It prints no finalizers, and a
    target carries neither namespace nor signature, so overloads share a name.
    """
    found = set()
    for line in block_after(text, "Active Harmony patches:"):
        target, separator, rest = line.partition(": ")
        if not separator or target == "Harmony versions present":
            continue
        pieces = PATCH_SECTION.split(rest)
        for label, names in zip(pieces[1::2], pieces[2::2]):
            for name in names.split(", "):
                name = name.strip()
                priority = PATCH_PRIORITY.match(name)
                if priority:
                    name = name[priority.end() :]
                if name:
                    found.add((target, PATCH_KINDS[label], int(priority[1]) if priority else None, name))
    return list(found)


def patch_key(patch: Patch) -> int:
    """A patch's dictionary key: 64 bits of a hash over its four printed parts."""
    target, kind, priority, method = patch
    digest = hashlib.blake2b(f"{target}\x1f{kind}\x1f{priority}\x1f{method}".encode("utf-8"), digest_size=8)
    return int.from_bytes(digest.digest(), "big", signed=True)


def namespace_of(method: str) -> str:
    """Namespace of a patch method printed as Namespace.Class.Method, '' when it has none."""
    parts = method.rsplit(".", 2)
    return parts[0] if len(parts) == 3 else ""
