import os

import pandas as pd
import psycopg
from psycopg.rows import dict_row


def load_results():
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row, connect_timeout=5) as connection:
        # Открываем транзакцию для чтения одного снимка базы
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        # Получаем десять последних фродовых транзакций
        fraud = connection.execute(
            """SELECT transaction_id, score, fraud_flag, stored_at
               FROM transaction_scores WHERE fraud_flag = 1
               ORDER BY id DESC LIMIT 10"""
        ).fetchall()
        # Получаем скоры последних ста транзакций для гистограммы
        scores = connection.execute(
            "SELECT score FROM transaction_scores ORDER BY id DESC LIMIT 100"
        ).fetchall()
    return pd.DataFrame(fraud, columns=["transaction_id", "score", "fraud_flag", "stored_at"]), pd.DataFrame(scores, columns=["score"])
