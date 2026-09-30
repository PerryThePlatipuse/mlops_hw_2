import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import os
import uuid
import psycopg2

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": str(uuid.uuid4()),
                    "data": row.to_dict()
                }
            )
            if idx % 1000 == 0 or idx == total_rows:
                progress_bar.progress(idx / total_rows)
            
        producer.flush()
        producer.close()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

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
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

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

st.header("Результаты скоринга")
if st.button("Посмотреть результаты"):
    try:
        with psycopg2.connect(os.environ['DATABASE_URL']) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'SELECT transaction_id, score, fraud_flag FROM scores '
                    'WHERE fraud_flag = 1 ORDER BY id DESC LIMIT 10'
                )
                frauds = cursor.fetchall()
                cursor.execute('SELECT score FROM scores ORDER BY id DESC LIMIT 100')
                scores = [row[0] for row in cursor.fetchall()]
    except psycopg2.Error:
        st.error('Не удалось получить результаты из PostgreSQL')
    else:
        st.subheader('Последние транзакции с fraud_flag = 1')
        if frauds:
            st.dataframe(pd.DataFrame(frauds, columns=['transaction_id', 'score', 'fraud_flag']), hide_index=True)
        else:
            st.info('Таких транзакций пока нет')

        st.subheader('Распределение скоров последних 100 транзакций')
        if scores:
            hist = pd.cut(pd.Series(scores), bins=[i / 10 for i in range(11)], include_lowest=True).value_counts(sort=False)
            chart = pd.DataFrame({'Интервал score': hist.index.astype(str), 'Транзакций': hist.values})
            st.bar_chart(chart, x='Интервал score', y='Транзакций')
        else:
            st.info('Результатов пока нет')
