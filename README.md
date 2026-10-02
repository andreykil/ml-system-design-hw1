# Real-Time Fraud Detection System

## Запуск проекта

Нужны работающий Docker и свободные порты **8501, 8080, 9095**.

В папку **`data/`** в корне репозитория положите исходный **`train.csv`**.

При старте статистики по `train` рассчитываются один раз в памяти.

Из корня проекта:

```bash
docker compose up --build -d
docker compose logs -f fraud_detector scores_writer
```

Должны появиться сообщения `Scorer ready` и `Scores writer ready`. При первом запуске скореру нужно время для обработки train.

- Приложение: http://localhost:8501
- Kafka UI: http://localhost:8080

В приложении есть кнопка **«Посмотреть результаты»**: появятся последние 10 фродовых транзакций и гистограмма скоров последних 100 транзакций. Скоринг идет отдельно от отправки, пока файл обрабатывается, результаты можно обновлять этой же кнопкой.

## Структура

```text
.
├── .gitignore                      
├── README.md                       
├── docker-compose.yaml             # Запуск сервисов, сеть и хранилища
├── data/                           # Данные добавляются вручную
│   └── train.csv                   # Статистики для препроцессинга
├── fraud_detector/                 # Сервис скоринга транзакций
│   ├── app/
│   │   └── app.py
│   ├── src/
│   │   ├── preprocessing.py
│   │   └── scorer.py
│   ├── models/
│   │   └── my_catboost.cbm
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── .dockerignore
│   └── .gitignore
├── interface/                      # Приложение Streamlit
│   ├── app.py
│   ├── db.py                       # Чтение результатов из PostgreSQL
│   ├── .streamlit/
│   │   └── config.toml
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .dockerignore
├── scores_writer/                  # Сервис записи результатов в БД
│   ├── app.py                      # Чтение scores и сохранение в PostgreSQL
│   ├── Dockerfile                  # Сборка образа writer
│   └── requirements.txt
└── postgres/
    └── init/
        └── 001_create_scores.sql    # Создание таблицы результатов и индекса
```
