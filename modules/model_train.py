import pandas as pd
import joblib
from utils.build_data import build_df
from utils.preprocessing import balance_dataset, feature_select
from utils.helpers import lgbm_fit, get_prediction
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score

#building data
X, y = build_df('train', '../sources/train/', '../datasets/')

X_train, X_val, y_train, y_val = train_test_split(X, y, random_state=42)

#balancing data
X_train_bal, y_train_bal = balance_dataset(X_train, y_train)
X_train_bal.columns = X.columns

#feature selection
X_train_bal = feature_select(X_train_bal)
X_val = feature_select(X_val)

#model_train
clf = lgbm_fit(X_train_bal, y_train_bal)
joblib.dump(clf, '../utils/classifier.mod')

#validation
y_score = get_prediction(X_val, clf)

print(f'F1 score: {f1_score(y_val, y_score)}')
# y_score.to_csv('../datasets/y_val_predicted.csv', header=False, sep=';', index=True)