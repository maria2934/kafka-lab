# src/consumer.py

from kafka import KafkaConsumer
import json

class KafkaSalesConsumer:
    def __init__(self, bootstrap_servers='kafka:9092', topic='sales-topic'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='sales-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start(self):
        print("👂 Потребитель слушает топик 'sales-topic'...")
        try:
            for message in self.consumer:
                sale = message.value
                print(f"\n✅ Получена продажа:")
                print(f"   Заказ #{sale['order_id']}")
                print(f"   Товар: {sale['product']} × {sale['quantity']}")
                print(f"   Цена: ${sale['price']}")
                print(f"   Регион: {sale['region']}")
                print(f"   Время: {sale['timestamp']}")
        except KeyboardInterrupt:
            print("\n🛑 Потребитель остановлен пользователем.")
        except Exception as e:
            print(f"❌ Ошибка при получении сообщений: {e}")

if __name__ == "__main__":
    consumer = KafkaSalesConsumer()
    consumer.start()
