from __future__ import annotations

import argparse
import json

from .pipeline import RetailPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Online retail warehouse ETL")
    parser.add_argument("--config", default="config/pipeline.json")
    parser.add_argument(
        "command", nargs="?", default="run", choices=("run", "check", "explain")
    )
    args = parser.parse_args()
    pipeline = RetailPipeline.from_config(args.config)
    if args.command == "run":
        result = pipeline.run()
    elif args.command == "check":
        result = pipeline.check_existing_warehouse()
    else:
        result = pipeline.explain_representative_query()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

