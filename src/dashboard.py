import streamlit as st
import json
import time
from kafka import KafkaConsumer
import pandas as pd

# Настройка страницы Streamlit
st.set_page_config(page_title="📊 Live Sales Dashboard", layout="wide")
st.title("🚀 Real-Time Sales Dashboard")
st.markdown("Отображение продаж в реальном времени через Kafka")

# Инициализация состояния
if 'sales_data' not in st.session_state:
    st.session_state.sales_data = []

# Подключение к Kafka
def create_kafka_consumer():
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                'sales-topic',
                bootstrap_servers='kafka:9092',  # 🔥 Здесь были кавычки!
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='dashboard-group'
            )
            st.success("✅ Подключено к Kafka")
            return consumer
        except Exception as e:
            st.warning(f"🔴 Подключение к Kafka не удалось, попытка {attempt + 1}/30: {e}")
            time.sleep(2)
    st.error("❌ Не удалось подключиться к Kafka после всех попыток")
    return None

# Создаём потребителя
consumer = create_kafka_consumer()

# Основной цикл
if consumer:
    placeholder = st.empty()
    for message in consumer:
        sale = message.value
        st.session_state.sales_data.append(sale)

        with placeholder.container():
            st.subheader("📊 Последняя продажа")
            col1, col2, col3 = st.columns(3)
            col1.metric("Товар", sale['product'])
            col2.metric("Цена ($)", sale['price'])
            col3.metric("Время", time.strftime('%H:%M:%S', time.localtime(sale['timestamp'])))

            st.subheader("📈 История продаж")
            df = pd.DataFrame(st.session_state.sales_data)
            st.bar_chart(df['price'].tail(20))
            st.dataframe(df.tail(10))
else:
    st.error("Не удалось запустить потребителя Kafka")
