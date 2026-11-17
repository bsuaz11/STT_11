"""Trip or release the STT_11 kill switch by hand, mid-session.

Every running seat polls its kill files once a tick (team/risk.py KillSwitch):

    python scripts/kill_switch.py on                   # all seats: no new orders
    python scripts/kill_switch.py on --flatten         # …and close every position
    python scripts/kill_switch.py on --seat stt_11_broker_1
    python scripts/kill_switch.py off [--seat ID]      # release the manual trip
    python scripts/kill_switch.py status

Automatic trips (session loss, reject storm, reconnects, stale feed) are not
released by `off`; the loss stop clears at the next session, the others time
out by themselves.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from team.risk import KILL_DIR


def _path(seat: str | None) -> pathlib.Path:
    return KILL_DIR / (f"KILL.{seat}" if seat else "KILL")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("action", choices=("on", "off", "status"))
    ap.add_argument("--seat", help="one seat id (default: every seat)")
    ap.add_argument("--flatten", action="store_true", help="also close every position")
    args = ap.parse_args(argv)

    if args.action == "on":
        KILL_DIR.mkdir(parents=True, exist_ok=True)
        _path(args.seat).write_text("flatten\n" if args.flatten else "stop\n")
        print(f"KILL ON ({'flatten' if args.flatten else 'stop sending'}) → {_path(args.seat)}")
    elif args.action == "off":
        try:
            _path(args.seat).unlink()
            print(f"KILL OFF → removed {_path(args.seat)}")
        except FileNotFoundError:
            print(f"no kill file at {_path(args.seat)}")
    else:
        files = sorted(KILL_DIR.glob("KILL*"))
        if not files:
            print(f"no manual kill set ({KILL_DIR})")
        for f in files:
            print(f"{f.name}: {f.read_text().strip()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
