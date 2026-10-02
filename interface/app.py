import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import os
import uuid
import altair as alt

from db import load_results

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

REQUIRED_COLUMNS = {
    'transaction_time', 'merch', 'cat_id', 'amount', 'gender', 'one_city',
    'us_state', 'lat', 'lon', 'population_city', 'jobs', 'merchant_lat', 'merchant_lon',
}


def validate_frame(df):
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f'Отсутствуют обязательные столбцы: {", ".join(sorted(missing))}')
    if df.empty:
        raise ValueError('CSV не содержит транзакций')

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        df = pd.read_csv(uploaded_file)
        validate_frame(df)
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    producer = None
    try:
        validate_frame(df)
        df = df.astype(object).where(pd.notna(df), None).reset_index(drop=True)
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, allow_nan=False).encode("utf-8"),
            security_protocol="PLAINTEXT",
            acks="all",
            retries=3,
            request_timeout_ms=10000,
            max_block_ms=10000
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        delivery_errors = []
        
        for idx, values in enumerate(df.itertuples(index=False, name=None)):
            row = dict(zip(df.columns, values))
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row.pop('transaction_id'),
                    "data": row
                }
            ).add_errback(delivery_errors.append)
            if delivery_errors:
                raise delivery_errors[0]
            # Обновляем прогресс раз в 1000 строк
            if (idx + 1) % 1000 == 0:
                progress_bar.progress((idx + 1) / total_rows)
            
        # Дожидаемся доставки всех сообщений
        producer.flush(timeout=60)
        if delivery_errors:
            raise delivery_errors[0]
        progress_bar.progress(1.0)
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False
    finally:
        if producer is not None:
            producer.close(timeout=10)

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    if st.session_state.uploaded_files[uploaded_file.name]["df"] is not None:
        st.success(f"Файл {uploaded_file.name} успешно загружен!")
    else:
        del st.session_state.uploaded_files[uploaded_file.name]

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

# Просмотр результатов скоринга
st.divider()
st.subheader("Результаты скоринга")
st.write('Последние записи определяются по порядку сохранения в PostgreSQL.')
if st.button('Посмотреть результаты'):
    try:
        fraud, scores = load_results()
    except Exception as error:
        st.error(f'Не удалось получить результаты: {error}')
    else:
        st.subheader('10 последних фродовых транзакций')
        if fraud.empty:
            st.info('Транзакций с fraud_flag = 1 пока нет.')
        else:
            st.dataframe(fraud, use_container_width=True, hide_index=True)
        st.subheader('Распределение скоров последних 100 транзакций')
        if scores.empty:
            st.info('Результатов пока нет. Отправьте транзакции и нажмите «Посмотреть результаты» ещё раз.')
        else:
            st.caption(f'Транзакций в выборке: {len(scores)}')
            chart = alt.Chart(scores).mark_bar().encode(
                x=alt.X('score:Q', bin=alt.Bin(step=0.05, extent=[0, 1]),
                        scale=alt.Scale(domain=[0, 1]), title='Скор модели'),
                y=alt.Y('count():Q', title='Количество транзакций'),
                tooltip=[alt.Tooltip('count():Q', title='Количество')],
            )
            st.altair_chart(chart, use_container_width=True)
