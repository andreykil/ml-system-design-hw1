import json
import logging
import os
from pathlib import Path
import signal
import sys

import pandas as pd
from confluent_kafka import Consumer, KafkaException, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from preprocessing import Preprocessor, load_train_data, run_preproc
from scorer import make_pred

logger = logging.getLogger(__name__)


class ProcessingService:
    def __init__(self):
        # Загружаем train и готовим статистики для препроцессинга
        self.preprocessor = Preprocessor(load_train_data())
        self.output_topic = os.getenv('KAFKA_SCORING_TOPIC', 'scores')
        servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        # Подключаемся к Kafka с ручным подтверждением сообщений
        self.consumer = Consumer({
            'bootstrap.servers': servers,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        self.consumer.subscribe([os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')])
        # Настраиваем отправку результатов в Kafka
        self.producer = Producer({
            'bootstrap.servers': servers,
            'enable.idempotence': True,
            'delivery.timeout.ms': 30000,
        })
        self.running = True

    def stop(self, signum, frame):
        # Останавливаем цикл обработки
        self.running = False

    def score_message(self, value):
        # Разбираем JSON транзакции
        data = json.loads(value)
        if not isinstance(data, dict):
            raise ValueError('Expected a transaction object')
        # Извлекаем ID и проверяем поля сообщения
        transaction_id = data.get('transaction_id')
        if not isinstance(transaction_id, str) or not transaction_id.strip():
            raise ValueError('transaction_id must be a nonempty string')
        if not isinstance(data.get('data'), dict):
            raise ValueError('data must contain a transaction object')
        # Собираем DataFrame одной транзакции и готовим признаки
        features = run_preproc(self.preprocessor, pd.DataFrame([data['data']]))
        # Получаем скор модели и флаг фрода
        result = make_pred(features).iloc[0]
        return {'transaction_id': transaction_id, 'score': float(result['score']),
                'fraud_flag': int(result['fraud_flag'])}

    def publish(self, result):
        errors = []

        # Собираем ошибки доставки результата
        def delivered(error, message):
            if error is not None:
                errors.append(error)

        # Отправляем JSON результата с ID транзакции в качестве ключа
        self.producer.produce(
            self.output_topic, key=result['transaction_id'],
            value=json.dumps(result, allow_nan=False), on_delivery=delivered,
        )
        pending = self.producer.flush(35)
        if pending or errors:
            raise RuntimeError(f'Score delivery failed: pending={pending}, errors={errors}')

    def process_messages(self):
        logger.info('Scorer ready; publishing to %s', self.output_topic)
        try:
            while self.running:
                # Читаем очередное сообщение из Kafka
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                try:
                    # получаем результат скоринга
                    result = self.score_message(msg.value())
                except (ValueError, TypeError, UnicodeDecodeError):
                    # Записываем ошибку в лог и пропускаем некорректное сообщение
                    logger.exception('Skipping invalid transaction at %s:%s:%s', msg.topic(), msg.partition(), msg.offset())
                    self.consumer.commit(message=msg, asynchronous=False)
                    continue
                # Отправляем результат в выходной топик
                self.publish(result)
                self.consumer.commit(message=msg, asynchronous=False)
                logger.info('Scored transaction %s', result['transaction_id'])
        finally:
            self.consumer.close()
            self.producer.flush(5)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    service = ProcessingService()
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)
    service.process_messages()
