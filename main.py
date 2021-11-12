import pandas as pd
import joblib
from utils.build_data import build_df
from utils.preprocessing import feature_select
from utils.helpers import get_prediction

X_test, _ = build_df('test', 'sources/test/', 'datasets/')
print(X_test['session_amt_1'].head())

#feature selection
X_test = feature_select(X_test)
print(X_test.head())

clf = joblib.load('utils/classifier.mod')

y_test = get_prediction(X_test, clf)
y_test.to_csv('datasets/y_predicted.csv', header=False, sep=';', index=True)
