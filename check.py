#!/usr/bin/env python3
"""Обязательная статическая проверка целостности price-monitor."""
import ast
import builtins
from pathlib import Path

SOURCE = Path(__file__).with_name("app.py")
NEED = {
    "brand_of", "db", "init_db", "fetch", "strip_tags", "to_num", "head_of",
    "is_part", "stock_of", "parse_shop", "parse_entero", "parse_card", "model_code",
    "sitemap_urls", "discover_entero", "discover_slug", "discover_brandpage",
    "cmd_discover", "interleave", "cmd_daily", "latest", "build_table", "cmd_compare",
    "push_snapshot", "archive_compare_run", "fetch_policy", "refresh_eur_rate",
    "calculate_price_item", "cmd_calculate", "cmd_report", "send_mail", "log", "_step", "cmd_selfupdate",
}


def names_from_target(node):
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, (ast.Tuple, ast.List)):
        return set().union(*(names_from_target(item) for item in node.elts))
    return set()


def main():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
    functions = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    classes = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
    missing = sorted(NEED - functions)
    if missing:
        raise SystemExit("missing required functions: " + ", ".join(missing))

    run_ok = set()
    dispatch = set()
    defined = set(dir(builtins)) | functions | classes
    called = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                defined.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                defined |= names_from_target(target)
            if any(isinstance(t, ast.Name) and t.id == "RUN_OK" for t in node.targets):
                run_ok = {x.value for x in node.value.elts if isinstance(x, ast.Constant)}
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            called.add(node.func.id)
        elif isinstance(node, ast.Dict):
            keys = {key.value for key in node.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)}
            if run_ok & keys:
                dispatch |= keys
        elif isinstance(node, ast.ExceptHandler) and node.type is None:
            raise SystemExit("bare except is forbidden")

    missing_dispatch = sorted(run_ok - dispatch)
    if missing_dispatch:
        raise SystemExit("RUN_OK commands absent from dispatcher: " + ", ".join(missing_dispatch))
    unknown_calls = sorted(called - defined)
    if unknown_calls:
        raise SystemExit("calls to undefined names: " + ", ".join(unknown_calls))
    compile(SOURCE.read_bytes(), str(SOURCE), "exec")
    print("check.py: OK — %d required functions, %d commands" % (len(NEED), len(run_ok)))


if __name__ == "__main__":
    main()
