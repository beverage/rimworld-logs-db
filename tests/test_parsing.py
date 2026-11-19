"""Reading the mod list, game version and Harmony patches off a log."""

from rimworld_logs_db import parsing

LOG = (
    "Log uploaded on Saturday, November 14, 2026, 1:58:50 PM\r\n"
    "Loaded mods:\r\n"
    "Harmony(brrainz.harmony)[mv:3.0.1.0]: 0Harmony(2.4.3), HarmonyMod(2.4.2)\r\n"
    "Core(Ludeon.RimWorld): (no assemblies)\r\n"
    "Odyssey(Ludeon.RimWorld.Odyssey): (no assemblies)\r\n"
    "Royalty(ludeon.rimworld.royalty): (no assemblies)\r\n"
    "Yayo's Animation (Continued)(com.yayo.yayoAni.continued): 0PrepatcherAPI(1.2.0), yayoAni(1.0.0)\r\n"
    "Odd (v2): Name(odd.name)[ov:1.0]: Thing(av:1.0.0,fv:1.2.0)\r\n"
    "\r\n"
    "Active Harmony patches:\r\n"
    "Not.A.Mod(should.not.count): PRE: x\r\n"
    "\r\n"
    "Log file contents:\r\n"
    "[00:00:01] RimWorld 1.7.5011 rev1082\r\n"
    "(log trimmed to 10,000 lines. Use publishing options to upload the full log)"
)

PATCH_LOG = (
    "Loaded mods:\nCore(Ludeon.RimWorld): (no assemblies)\n\n"
    "Active Harmony patches:\n"
    "Ability.PreActivate: post: A.B.Post\n"
    "Pawn.Tick: PRE: [800]X.Y.Pre, Z.Class+Nested.Pre post: Q.R.Post TRANS: T.U.Trans\n"
    "Pawn.Tick: PRE: [800]X.Y.Pre\n"
    "Dialog_Trade.Close: (no patches)\n"
    "FleckSystemBase`1.ForceDraw: PRE: RimThreadedTTR.FleckPatch`1.ForceDrawPrefix\n"
    "Low.Priority: post: [-5]Neg.Class.Post\n"
    "Harmony versions present: 2.4.3.0: some.owner, other.owner; 2.3.3.0: x.y\n"
    "\n"
    "Log file contents:\nSomething.Else: PRE: Not.A.Patch\n"
)


def test_mod_list_takes_the_package_id_even_when_the_name_has_parentheses():
    assert parsing.parse_log(LOG)["mods"] == [
        "brrainz.harmony",
        "Ludeon.RimWorld",
        "Ludeon.RimWorld.Odyssey",
        "ludeon.rimworld.royalty",
        "com.yayo.yayoAni.continued",
        "odd.name",
    ]


def test_mod_list_stops_at_the_blank_line():
    assert "should.not.count" not in parsing.parse_log(LOG)["mods"]


def test_dlc_is_recognised_whatever_the_case_of_its_package_id():
    assert parsing.parse_log(LOG)["dlcs"] == ["Odyssey", "royalty"]


def test_game_version_is_found_behind_a_time_prefix():
    facts = parsing.parse_log(LOG)
    assert (facts["game_version"], facts["game_rev"]) == ("1.7.5011", 1082)


def test_trim_marker_is_noticed():
    assert parsing.parse_log(LOG)["trimmed"] is True


def test_text_with_no_header_has_no_mods():
    facts = parsing.parse_log("just text")
    assert (facts["mod_count"], facts["game_version"], facts["trimmed"]) == (0, None, False)


def test_patches_are_read_per_kind_with_priority():
    assert set(parsing.parse_patches(PATCH_LOG)) == {
        ("Ability.PreActivate", "postfix", None, "A.B.Post"),
        ("Pawn.Tick", "prefix", 800, "X.Y.Pre"),
        ("Pawn.Tick", "prefix", None, "Z.Class+Nested.Pre"),
        ("Pawn.Tick", "postfix", None, "Q.R.Post"),
        ("Pawn.Tick", "transpiler", None, "T.U.Trans"),
        ("FleckSystemBase`1.ForceDraw", "prefix", None, "RimThreadedTTR.FleckPatch`1.ForceDrawPrefix"),
        ("Low.Priority", "postfix", -5, "Neg.Class.Post"),
    }


def test_a_patch_listed_twice_is_returned_once():
    patches = parsing.parse_patches(PATCH_LOG)
    assert len(patches) == len(set(patches))


def test_text_with_no_patch_block_has_no_patches():
    assert parsing.parse_patches("nothing here") == []


def test_namespace_drops_the_class_and_method():
    assert parsing.namespace_of("VFEEmpire.HarmonyPatches.Patch_Conceit+Work.Transpiler") == "VFEEmpire.HarmonyPatches"


def test_a_class_with_no_namespace_gives_an_empty_one():
    assert parsing.namespace_of("Class.Method") == ""


def test_patch_key_is_stable_and_fits_a_signed_64_bit_integer():
    key = parsing.patch_key(("a", "prefix", None, "b"))
    assert key == parsing.patch_key(("a", "prefix", None, "b"))
    assert -(2**63) <= key < 2**63


def test_priority_is_part_of_the_patch_key():
    assert parsing.patch_key(("a", "prefix", None, "b")) != parsing.patch_key(("a", "prefix", 800, "b"))


def test_invalid_utf8_is_replaced_and_reported():
    assert parsing.decode(b"ok") == ("ok", True)
    text, valid = parsing.decode(b"bad \xff byte")
    assert valid is False
    assert text.startswith("bad ") and text.endswith(" byte")
