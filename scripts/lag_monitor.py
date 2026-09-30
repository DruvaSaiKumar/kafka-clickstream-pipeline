"""Print consumer group lag for a topic, the way an operator would check if a consumer is keeping up.

python -m scripts.lag_monitor --topic clickstream.raw --group clickstream-sessionizer
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from clickstream.admin import consumer_lag  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", action="append", dest="topics", required=True)
    parser.add_argument("--group", required=True)
    args = parser.parse_args()

    lag = consumer_lag(args.bootstrap, args.group, args.topics)
    for topic, n in lag.items():
        print(f"{topic}: {n} message(s) behind")
    return 1 if any(n > 0 for n in lag.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
