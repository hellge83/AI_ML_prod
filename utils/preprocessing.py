import time
from utils.helpers import time_format
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE

def prepare_dataset(dataset,
                    dataset_type='train',
                    dataset_path='../datasets/',
                    inter_list=None):
    start_t = time.time()
    print('Dealing with missing values, outliers, categorical features...')

    # Профили
    dataset['age'] = dataset['age'].fillna(dataset['age'].median())
    dataset['gender'] = dataset['gender'].fillna(dataset['gender'].mode()[0])  # Male
    dataset.loc[~dataset['gender'].isin(['M', 'F']), 'gender'] = dataset['gender'].mode()[0]
    dataset['gender'] = dataset['gender'].map({'M': 1., 'F': 0.})
    dataset.loc[(dataset['age'] > 80) | (dataset['age'] < 7), 'age'] = round(dataset['age'].median())
    dataset.loc[dataset['days_between_fl_df'] < 0, 'days_between_fl_df'] = 0
    dataset.loc[dataset['days_between_reg_fl'] < 0, 'days_between_reg_fl'] = 0
    # Пинги
    for period in range(1, len(inter_list) + 1):
        col = f'avg_min_ping_{period}'
        dataset.loc[(dataset[col] < 0) |
                    (dataset[col].isnull()), col] = dataset.loc[dataset[col] >= 0][col].median()
    # Сессии и прочее
    dataset.fillna(0, inplace=True)
    dataset.to_csv(f'{dataset_path}dataset_{dataset_type}.csv', sep=';', index=True, index_label='user_id')

    print(f'Dataset is successfully prepared and saved to {dataset_path}, run time (dealing with bad values): {time_format(time.time( ) -start_t)}')


def scaler_fit(X):
    print('Scaling data...')
    scl = StandardScaler()
    scl.fit(X)
    print('Done!')
    return scl


def balance_dataset(X, y):
    print('Balancing data...')
    sm = SMOTE(random_state=42, sampling_strategy=0.15)
    X, y = sm.fit_resample(X, y)
    print('Done!')
    return X, y


def feature_select(X):
    good_features = ['session_amt_1',
                     'disconnect_amt_1',
                     'age',
                     'level',
                     'session_amt_5',
                     'trans_amt_2',
                     'sess_with_abusers_amt_1',
                     'trans_amt_1',
                     'leavings_rate_1',
                     'session_player_1',
                     'session_amt_4',
                     'session_amt_2',
                     'reports_amt_1',
                     'gender',
                     'trans_amt_5',
                     'trans_amt_3',
                     'sess_with_abusers_amt_2',
                     'session_amt_3',
                     'win_rate_1',
                     'trans_amt_4',
                     'reports_amt_2',
                     'disconnect_amt_5',
                     'reports_amt_4',
                     'days_between_fl_df',
                     'avg_min_ping_1',
                     'kd_1',
                     'disconnect_amt_2',
                     'reports_amt_5',
                     'reports_amt_3',
                     'has_return_date',
                     'has_phone_number',
                     'disconnect_amt_3',
                     'sess_with_abusers_amt_5',
                     'disconnect_amt_4',
                     'session_player_5',
                     'kd_5',
                     'gold_spent_1',
                     'session_player_2',
                     'session_player_3',
                     'session_player_4',
                     'silver_spent_4',
                     'silver_spent_2',
                     'kd_3',
                     'avg_min_ping_2',
                     'win_rate_3',
                     'leavings_rate_2',
                     'pay_amt_1',
                     'pay_amt_5',
                     'win_rate_5',
                     'donate_total',
                     'silver_spent_1',
                     'sess_with_abusers_amt_3',
                     'leavings_rate_5',
                     'avg_min_ping_5',
                     'avg_min_ping_3',
                     'leavings_rate_3',
                     'gold_spent_2',
                     'silver_spent_5',
                     'gold_spent_4',
                     'gold_spent_5',
                     'silver_spent_3',
                     'gold_spent_3',
                     'days_between_reg_fl',
                     'avg_min_ping_4',
                     'pay_amt_2']
    return X[good_features]
