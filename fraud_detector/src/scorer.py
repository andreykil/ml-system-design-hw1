import os
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

MODEL_PATH = Path(__file__).resolve().parents[1] / 'models/my_catboost.cbm'
# Import model
model = CatBoostClassifier()
model.load_model(str(MODEL_PATH))
model_th = float(os.getenv('FRAUD_THRESHOLD', '0.98'))
if not 0 <= model_th <= 1:
    raise ValueError('FRAUD_THRESHOLD must be in [0, 1]')


def make_pred(dt, source_info='kafka'):
    # Меняем формат категориальных фичей на string перед скорингом
    features = dt[model.feature_names_].copy()
    for index in model.get_cat_feature_indices():
        col = model.feature_names_[index]
        features[col] = features[col].astype(str)
    # Считаем вероятность фрода на CPU
    scores = model.predict_proba(features, thread_count=1, task_type='CPU')[:, 1]
    return pd.DataFrame({'score': scores, 'fraud_flag': (scores > model_th).astype(int)})
