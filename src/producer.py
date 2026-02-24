import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

class KafkaSalesProducer:
    def __init__(self, bootstrap_servers='kafka:9092', max_retries=30, retry_interval=2):
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.producer = None
        self.products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Phone']
        self.connect()

    def connect(self):
        """Подключаемся к Kafka с повторными попытками"""
        for attempt in range(self.max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    api_version_auto_timeout_ms=30000
                )
                # Проверим, можем ли мы получить метрики (простой способ проверить живость)
                self.producer.metrics()
                print(f"✅ Успешно подключились к Kafka: {self.bootstrap_servers}")
                return
            except NoBrokersAvailable:
                print(f"🔴 Брокер недоступен, попытка {attempt + 1}/{self.max_retries}. Ждём {self.retry_interval} сек...")
                time.sleep(self.retry_interval)
            except Exception as e:
                print(f"❌ Ошибка подключения: {e}")
                time.sleep(self.retry_interval)

        raise Exception("Не удалось подключиться к Kafka после всех попыток")

    def send_sale(self):
        sale = {
            'product': random.choice(self.products),
            'price': random.randint(50, 2000),
            'timestamp': time.time()
        }
        try:
            self.producer.send('sales-topic', sale)
            print(f"✅ Отправлено: {sale}")
        except Exception as e:
            print(f"❌ Ошибка отправки: {e}")

    def run(self):
        while True:
            self.send_sale()
            time.sleep(1)

if __name__ == "__main__":
    producer = KafkaSalesProducer()
    producer.run()
