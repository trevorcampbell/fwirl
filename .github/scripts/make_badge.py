"""Write a shields.io endpoint JSON file (https://shields.io/badges/endpoint-badge)."""
import json
import sys


def color(percent):
    for threshold, name in ((95, "brightgreen"), (85, "green"), (75, "yellowgreen"),
                            (60, "yellow"), (40, "orange")):
        if percent >= threshold:
            return name
    return "red"


def main(label, percent, path):
    percent = float(percent)
    badge = {
        "schemaVersion": 1,
        "label": label,
        "message": f"{percent:.0f}%",
        "color": color(percent),
    }
    with open(path, "w") as f:
        json.dump(badge, f)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("usage: make_badge.py LABEL PERCENT OUTPUT_PATH")
    main(*sys.argv[1:])
