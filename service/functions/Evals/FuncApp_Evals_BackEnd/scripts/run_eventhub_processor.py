from __future__ import annotations

import argparse
import logging
import os

from config.loader import load_config
from data.cosmos_client import CosmosDbClient
from telemetry.processor import TelemetryEventProcessor, run_eventhub_processor_loop


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Event Hubs telemetry stream processor.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to backend config file.")
    parser.add_argument("--eventhub-connection-string", default=os.getenv("EVENTHUB_CONNECTION_STRING"))
    parser.add_argument("--eventhub-name", default=os.getenv("EVENTHUB_NAME"))
    parser.add_argument("--consumer-group", default=os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default"))
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if not args.eventhub_connection_string or not args.eventhub_name:
        raise ValueError("Event Hubs connection string and name are required.")

    cfg = load_config(args.config)
    if cfg.cosmos is None:
        raise ValueError("Cosmos config is required.")

    cosmos = CosmosDbClient(cfg.cosmos)
    processor = TelemetryEventProcessor(cosmos)
    run_eventhub_processor_loop(
        processor=processor,
        connection_string=args.eventhub_connection_string,
        eventhub_name=args.eventhub_name,
        consumer_group=args.consumer_group,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
