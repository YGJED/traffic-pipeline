import argparse
import json
import os
import sys
from dotenv import load_dotenv, find_dotenv
from kafka import KafkaConsumer


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka consumer for traffic pipeline testing")

    parser.add_argument(
        "--topic",
        type=str,
        default=os.getenv("KAFKA_TOPIC", "road-segments"),
        help="Kafka topic to consume from"
    )

    parser.add_argument(
        "--bootstrap",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap server address"
    )

    parser.add_argument(
        "--group-id",
        type=str,
        default=os.getenv("KAFKA_CONSUMER_GROUP", "traffic-pipeline-test-consumer"),
        help="Kafka consumer group id"
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=1000,
        help="Poll timeout in milliseconds"
    )

    return parser.parse_args()


def main():
    load_dotenv(find_dotenv())
    args = parse_args()

    print(f"Connecting to Kafka bootstrap server: {args.bootstrap}")
    print(f"Consuming topic: {args.topic}")

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    print("Consumer started. Press Ctrl+C to exit.")

    try:
        while True:
            records = consumer.poll(timeout_ms=args.timeout)
            if not records:
                continue

            for tp, msgs in records.items():
                for message in msgs:
                    print("---")
                    print(f"topic={message.topic}", f"partition={message.partition}", f"offset={message.offset}")
                    print(f"key={message.key}")
                    print("value=")
                    print(json.dumps(message.value, indent=2, sort_keys=True, default=str))

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
