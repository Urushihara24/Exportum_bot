"""Behavior-preserving contracts for the legacy runtime before aiogram migration.

The production ``main.py`` cannot be imported safely in tests because importing it
creates bot/runtime objects and requires production-style environment variables.
These tests therefore parse the source and execute only pure status helpers and
status constants in an isolated namespace.
"""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "main.py"

FUNCTIONS = {
    "normalize_status_key",
    "normalize_transition_status",
    "is_pull_open_status",
    "is_request_open_for_offers",
    "is_request_open_for_expeditor",
    "is_logistic_role",
    "is_expeditor_role",
}

CONSTANTS = {
    "OPEN_LOGISTIC_OFFER_STATUSES",
    "SELECTED_LOGISTIC_OFFER_STATUSES",
    "MUTABLE_LOGISTIC_OFFER_STATUSES",
    "OPEN_EXPEDITOR_OFFER_STATUSES",
    "SELECTED_EXPEDITOR_OFFER_STATUSES",
    "MUTABLE_EXPEDITOR_OFFER_STATUSES",
}


def _assigned_names(node: ast.Assign) -> set[str]:
    names: set[str] = set()
    for target in node.targets:
        if isinstance(target, ast.Name):
            names.add(target.id)
    return names


def _load_contract_namespace() -> dict[str, object]:
    tree = ast.parse(MAIN.read_text(encoding="utf-8"), filename=str(MAIN))
    selected: list[ast.stmt] = []

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in FUNCTIONS:
                selected.append(node)
        elif isinstance(node, ast.Assign) and (_assigned_names(node) & CONSTANTS):
            selected.append(node)

    module = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(module)

    namespace: dict[str, object] = {}
    exec(compile(module, str(MAIN), "exec"), namespace)

    missing = (FUNCTIONS | CONSTANTS) - namespace.keys()
    assert not missing, f"status contract symbols disappeared from main.py: {sorted(missing)}"
    return namespace


def test_transition_status_normalizes_legacy_cancelled_aliases():
    ns = _load_contract_namespace()
    normalize = ns["normalize_transition_status"]

    assert normalize(" canceled ") == "cancelled"
    assert normalize("ОТМЕНЁН") == "cancelled"
    assert normalize("Assigned") == "assigned"
    assert normalize(None) == ""


def test_pull_open_status_contract():
    ns = _load_contract_namespace()
    is_open = ns["is_pull_open_status"]

    for status in ("active", "open", "collecting", "processing", "filling", "Активен"):
        assert is_open(status), status

    for status in ("completed", "cancelled", "sold", "closed", None):
        assert not is_open(status), status


def test_request_offer_stage_contracts_are_distinct():
    ns = _load_contract_namespace()
    logistics_open = ns["is_request_open_for_offers"]
    expeditor_open = ns["is_request_open_for_expeditor"]

    for status in ("pending", "active", "has_offers", "new", "open"):
        assert logistics_open(status), status
        assert expeditor_open(status), status

    assert not logistics_open("assigned")
    assert expeditor_open("assigned")
    assert expeditor_open("expeditor_selected")

    for status in ("in_progress", "completed", "cancelled", "rejected"):
        assert not logistics_open(status), status
        assert not expeditor_open(status), status


def test_offer_status_groups_preserve_selected_and_mutable_semantics():
    ns = _load_contract_namespace()

    expected_open = {"pending", "active", "new", "open"}
    expected_selected = {"accepted", "assigned", "in_progress", "selected", "reserved"}

    assert ns["OPEN_LOGISTIC_OFFER_STATUSES"] == expected_open
    assert ns["OPEN_EXPEDITOR_OFFER_STATUSES"] == expected_open
    assert ns["SELECTED_LOGISTIC_OFFER_STATUSES"] == expected_selected
    assert ns["SELECTED_EXPEDITOR_OFFER_STATUSES"] == expected_selected
    assert ns["MUTABLE_LOGISTIC_OFFER_STATUSES"] == expected_open | expected_selected
    assert ns["MUTABLE_EXPEDITOR_OFFER_STATUSES"] == expected_open | expected_selected
    assert "rejected" not in ns["MUTABLE_LOGISTIC_OFFER_STATUSES"]
    assert "completed" not in ns["MUTABLE_EXPEDITOR_OFFER_STATUSES"]


def test_legacy_role_aliases_remain_supported():
    ns = _load_contract_namespace()
    is_logistic = ns["is_logistic_role"]
    is_expeditor = ns["is_expeditor_role"]

    for role in ("logistic", "logistics", "logist", "ЛОГИСТ"):
        assert is_logistic(role), role
    for role in ("expeditor", "expeditor_broker", "экспедитор", "broker", "БРОКЕР"):
        assert is_expeditor(role), role

    assert not is_logistic("exporter")
    assert not is_expeditor("farmer")
