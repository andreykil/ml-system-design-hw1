FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Создаем папку для настроек Streamlit
RUN mkdir -p /root/.streamlit

# Копируем настройки Streamlit
COPY .streamlit/config.toml /root/.streamlit/config.toml

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
