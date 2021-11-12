import time
from datetime import timedelta
import lightgbm as lgbm
import pandas as pd


def time_format(sec):
    '''
    Time formatting
    '''
    return str(timedelta(seconds=sec))


def lgbm_fit(X, y):
    '''
    Fit classifier
    '''
    print('Fitting classifier...')
    clf = lgbm.LGBMClassifier(random_state=42,
                              # bagging_freq=5,
                              # num_leaves=1700,
                              # num_iterations=1150,
                              # min_data_in_leaf=3,
                              # max_depth=8,
                              # learning_rate=0.05,
                              # lambda_l2=0.0,
                              # lambda_l1=2.2222222222222223,
                              # feature_fraction=0.8888888888888888,
                              # bagging_fraction=0.9777777777777777
                              )
    clf.fit(X, y)
    print('Done!')
    return clf


def get_prediction(X, clf):
    y_score = clf.predict_proba(X)[:, 1]
    y_pred = y_score > 0.28554
    y_pred = y_pred * 1
    y_pred = pd.Series(y_pred, index=X.index)
    print('Predictions are ready')
    return y_pred


def etl_loop(func):
    '''
    Decorator for ETL process
    '''
    def _func(*args, **kwargs):
        _max_iter_cnt = 100
        for i in range(_max_iter_cnt):
            try:
                start_t = time.time()
                res = func(*args, **kwargs)
                run_time = time_format(time.time() - start_t)
                print('Run time "{}": {}'.format(func.__name__, run_time))
                return res
            except Exception as er:
                run_time = time_format(time.time() - start_t)
                print('Run time "{}": {}'.format(func.__name__, run_time))
                print('-'*50)
                print(er, '''Try № {}'''.format(i + 1))
                print('-'*50)
        raise Exception('Max error limit exceeded: {}'.format(_max_iter_cnt))
    return _func
