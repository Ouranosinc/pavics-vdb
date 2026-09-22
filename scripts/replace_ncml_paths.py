#!/usr/bin/env python3
"""Replace path substrings in .ncml files.

Usage examples:
  Dry-run (show changes):
    python scripts/replace_ncml_paths.py /path/to/search

  Apply changes (create backups):
    python scripts/replace_ncml_paths.py /path/to/search --apply

Options:
  --mode xml|text   : Parse XML and replace in attributes/text, or do raw text replace (default: xml)
  --old OLD         : Old substring to replace (default: /pavics-data/)
  --new NEW         : New substring (default: /pavics-data/disk3/)
  --backup          : Create a backup copy before modifying (default: True)
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
import xml.etree.ElementTree as ET
import sys


def replace_text_read_write(path: Path, old: str, new: str, apply: bool, backup: bool) -> int:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count == 0:
        return 0
    if not apply:
        return count
    if backup:
        bak = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(path, bak)
    new_text = text.replace(old, new)
    path.write_text(new_text, encoding="utf-8")
    return count


def replace_in_xml(path: Path, old: str, new: str, apply: bool, backup: bool) -> int:
    tree = ET.parse(path)
    root = tree.getroot()
    # If the document uses a namespace, register it as the default namespace
    # so ElementTree writes it without a generated prefix (ns0:)
    if root.tag.startswith("{"):
        uri = root.tag.split("}", 1)[0].lstrip("{")
        try:
            ET.register_namespace("", uri)
        except Exception:
            # ignore if registration fails for any reason
            pass
    changed = 0
    for elem in root.iter():
        # attributes
        for k, v in list(elem.attrib.items()):
            occ = v.count(old)
            if occ:
                changed += occ
                if apply:
                    elem.attrib[k] = v.replace(old, new)
        # text
        if elem.text:
            occ = elem.text.count(old)
            if occ:
                changed += occ
                if apply:
                    elem.text = elem.text.replace(old, new)
        # tail
        if elem.tail:
            occ = elem.tail.count(old)
            if occ:
                changed += occ
                if apply:
                    elem.tail = elem.tail.replace(old, new)

    if changed and apply:
        if backup:
            bak = path.with_suffix(path.suffix + ".bak")
            shutil.copy2(path, bak)
        tree.write(path, encoding="utf-8", xml_declaration=True)
    return changed


def walk_and_replace(base: Path, old: str, new: str, mode: str, apply: bool, backup: bool) -> int:
    total = 0
    for p in base.rglob("*.ncml"):
        try:
            if mode == "text":
                count = replace_text_read_write(p, old, new, apply, backup)
            else:
                count = replace_in_xml(p, old, new, apply, backup)
            if count:
                action = "WILL change" if not apply else "Changed"
                print(f"{action}: {p} -> {count} occurrence(s)")
                total += count
        except Exception as e:
            print(f"Error processing {p}: {e}", file=sys.stderr)
    return total


def main(argv=None):
    p = argparse.ArgumentParser(description="Replace /pavics-data/ with /pavics-data/disk3/ in .ncml files")
    p.add_argument("path", nargs="?", default=".", help="Base path to search")
    p.add_argument("--old", default="/pavics-data/", help="Old substring to replace")
    p.add_argument("--new", default="/pavics-data/disk3/", help="New substring")
    p.add_argument("--mode", choices=("xml", "text"), default="xml", help="Replace mode: xml or text")
    p.add_argument("--apply", action="store_true", help="Actually write changes (default: dry-run)")
    p.add_argument("--no-backup", dest="backup", action="store_false", help="Do not create backups")
    args = p.parse_args(argv)

    base = Path(args.path)
    if not base.exists():
        print(f"Base path not found: {base}", file=sys.stderr)
        return 2

    total = walk_and_replace(base, args.old, args.new, args.mode, args.apply, args.backup)
    if total == 0:
        print("No occurrences found.")
    else:
        if args.apply:
            print(f"Done: replaced {total} occurrence(s) across .ncml files.")
        else:
            print(f"Dry-run: {total} occurrence(s) would be replaced. Rerun with --apply to write changes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
