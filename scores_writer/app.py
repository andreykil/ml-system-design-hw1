import json
import logging
import math
import os
import signal

import psycopg
from confluent_kafka import Consumer, KafkaException

logger = logging.getLogger(__name__)


def validate_score(data):
    # Проверяем поля и значения результата скоринга
    if not isinstance(data, dict) or set(data) != {"transaction_id", "score", "fraud_flag"}:
        raise ValueError("Expected transaction_id, score, fraud_flag")
    if not isinstance(data["transaction_id"], str) or not data["transaction_id"].strip():
        raise ValueError("transaction_id must be a nonempty string")
    score = data["score"]
    if type(score) not in (int, float) or not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError("score must be a finite number in [0, 1]")
    if type(data["fraud_flag"]) is not int or data["fraud_flag"] not in (0, 1):
        raise ValueError("fraud_flag must be 0 or 1")
    return data


def store_score(connection, data):
    # Сохраняем результат в БД, пропуская уже записанные ID
    with connection.transaction():
        connection.execute(
            """INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
               VALUES (%s, %s, %s)
               ON CONFLICT (transaction_id) DO NOTHING""",
            (data["transaction_id"], data["score"], data["fraud_flag"]),
        )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    # Подключаемся к kafka с ручным подтверждением сообщений
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": "scores-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    try:
        with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, connect_timeout=10) as connection:
            consumer.subscribe([os.getenv("KAFKA_SCORING_TOPIC", "scores")])
            logger.info("Scores writer ready")
            while running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                try:
                    # Разбираем один результат скоринга и проверяем его поля
                    data = validate_score(json.loads(msg.value()))
                except (ValueError, TypeError, UnicodeDecodeError):
                    # Записываем ошибку в лог и пропускаем некорректное сообщение
                    logger.exception("Skipping invalid score at %s:%s:%s", msg.topic(), msg.partition(), msg.offset())
                    consumer.commit(message=msg, asynchronous=False)
                    continue
                # сохраняем результат в postgres
                store_score(connection, data)
                # Подтверждаем обработку сообщения kafka
                consumer.commit(message=msg, asynchronous=False)
                logger.info("Stored transaction %s", data["transaction_id"])
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
