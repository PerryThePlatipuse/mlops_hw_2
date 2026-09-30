import logging
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')
model_dir = Path(__file__).resolve().parents[1] / 'models'
model = CatBoostClassifier()
model.load_model(str(model_dir / 'my_catboost.cbm'))
model_th = float((model_dir / 'my_catboost_threshold.txt').read_text())
logger.info('Pretrained model imported successfully...')


def make_pred(dt):
    score = model.predict_proba(dt[model.feature_names_])[:, 1]
    submission = pd.DataFrame({
        'score': score,
        'fraud_flag': (score >= model_th).astype(int),
    })
    return submission
