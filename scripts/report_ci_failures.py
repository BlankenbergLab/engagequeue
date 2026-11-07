"""Expose JUnit failure details as GitHub Actions annotations."""

from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path


def escape(value: str, *, property_value: bool = False) -> str:
    result = value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    if property_value:
        result = result.replace(":", "%3A").replace(",", "%2C")
    return result


def location(case: ET.Element) -> str:
    filename = case.get("file")
    if not filename:
        parts = [part for part in case.get("classname", "").split(".") if part]
        for count in range(len(parts), 0, -1):
            candidate = Path(*parts[:count]).with_suffix(".py")
            if candidate.is_file():
                filename = candidate.as_posix()
                break
    if not filename:
        return ""
    result = "file=" + escape(filename, property_value=True) + ","
    if (line := case.get("line", "")).isdigit():
        result += f"line={int(line) + 1},"
    return result


def report(paths: list[Path]) -> int:
    emitted = 0
    for path in paths:
        try:
            root = ET.parse(path).getroot()
        except (OSError, ET.ParseError) as exc:
            print("::warning::" + escape(f"Cannot read JUnit report {path}: {exc}"))
            continue
        for case in root.iter("testcase"):
            for failure in case:
                if failure.tag not in ("failure", "error"):
                    continue
                if emitted >= 10:
                    print("More test failures are available in the uploaded JUnit reports.")
                    return emitted
                title = ".".join(part for part in (case.get("classname"), case.get("name")) if part)
                message = "".join(failure.itertext()).strip() or failure.get("message", "Test failed")
                if len(message) > 12000:
                    message = message[:12000] + "\n[Truncated; see the uploaded JUnit report for the full traceback.]"
                print(
                    "::error "
                    + location(case)
                    + "title="
                    + escape(title or "Pytest failure", property_value=True)
                    + "::"
                    + escape(message)
                )
                emitted += 1
    return emitted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path, default=[Path("reports")])
    args = parser.parse_args(argv)
    reports = sorted(
        {
            report
            for path in args.paths
            for report in (path.rglob("*.xml") if path.is_dir() else [path])
            if report.is_file()
        }
    )
    count = report(reports)
    print(f"Reported {count} JUnit failure annotations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
