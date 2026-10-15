from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"
SOURCE = MAIN_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE, filename=str(MAIN_PATH))


def _import_pairs() -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for node in TREE.body:
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                pairs.add((node.module, alias.name))
    return pairs


def _decorator_attributes() -> list[str]:
    attributes: list[str] = []
    for node in ast.walk(TREE):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            if isinstance(target, ast.Attribute):
                attributes.append(target.attr)
    return attributes


def _pure_runtime_namespace(*function_names: str) -> dict[str, object]:
    wanted = set(function_names)
    selected = [
        node
        for node in TREE.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted
    ]
    selected_names = {node.name for node in selected}
    missing = wanted - selected_names
    assert not missing, f"Missing expected runtime functions: {sorted(missing)}"

    module = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace: dict[str, object] = {}
    exec(compile(module, filename=str(MAIN_PATH), mode="exec"), namespace)
    return namespace


def test_aiogram2_runtime_surface_is_explicit_before_migration() -> None:
    imports = _import_pairs()

    assert ("aiogram", "Bot") in imports
    assert ("aiogram", "Dispatcher") in imports
    assert ("aiogram.dispatcher", "FSMContext") in imports
    assert ("aiogram.contrib.fsm_storage.memory", "MemoryStorage") in imports
    assert ("aiogram.utils", "executor") in imports

    decorators = _decorator_attributes()
    assert "message_handler" in decorators
    assert "callback_query_handler" in decorators


def test_status_transition_contracts_without_importing_bot_runtime() -> None:
    namespace = _pure_runtime_namespace(
        "normalize_status_key",
        "normalize_transition_status",
        "is_pull_open_status",
        "is_request_open_for_offers",
        "is_request_open_for_expeditor",
    )

    normalize = namespace["normalize_transition_status"]
    is_pull_open = namespace["is_pull_open_status"]
    is_request_open = namespace["is_request_open_for_offers"]
    is_request_open_for_expeditor = namespace["is_request_open_for_expeditor"]

    assert normalize(" CANCELED ") == "cancelled"
    assert normalize(None) == ""

    assert is_pull_open("active") is True
    assert is_pull_open(" OPEN ") is True
    assert is_pull_open("completed") is False

    assert is_request_open("pending") is True
    assert is_request_open("cancelled") is False

    assert is_request_open_for_expeditor("assigned") is True
    assert is_request_open_for_expeditor("completed") is False


def test_role_alias_contracts_without_importing_bot_runtime() -> None:
    namespace = _pure_runtime_namespace("is_logistic_role", "is_expeditor_role")
    is_logistic = namespace["is_logistic_role"]
    is_expeditor = namespace["is_expeditor_role"]

    assert is_logistic("logistic") is True
    assert is_logistic("logistics") is True
    assert is_logistic("логист") is True
    assert is_logistic("farmer") is False

    assert is_expeditor("expeditor") is True
    assert is_expeditor("broker") is True
    assert is_expeditor("брокер") is True
    assert is_expeditor("farmer") is False
