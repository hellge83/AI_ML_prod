import pandas as pd
import time
from datetime import datetime
from utils.helpers import time_format


def build_dataset_raw(churned_start_date='2019-01-01',
                      churned_end_date='2019-02-01',
                      inter_list=[(1, 7), (8, 14)],
                      raw_data_path='train/',
                      # dataset_path='/datasets/',
                      mode='train'):
    start_t = time.time()
    if mode == 'train':
        dataset_path = '../datasets/'
    elif mode == 'test':
        dataset_path = 'datasets/'

    sample = pd.read_csv(f'{raw_data_path}sample.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    profiles = pd.read_csv(f'{raw_data_path}profiles.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    payments = pd.read_csv(f'{raw_data_path}payments.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    reports = pd.read_csv(f'{raw_data_path}reports.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    abusers = pd.read_csv(f'{raw_data_path}abusers.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    logins = pd.read_csv(f'{raw_data_path}logins.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    pings = pd.read_csv(f'{raw_data_path}pings.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    sessions = pd.read_csv(f'{raw_data_path}sessions.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')
    shop = pd.read_csv(f'{raw_data_path}shop.csv', sep=';', na_values=['\\N', 'None'], encoding='utf-8')

    print(f'Run time (reading csv files): {time_format(time.time() - start_t)}')
    # -----------------------------------------------------------------------------------------------------
    print('NO dealing with outliers, missing values and categorical features...')
    # -----------------------------------------------------------------------------------------------------
    # На основании дня отвала (last_login_dt) строим признаки, которые описывают активность игрока перед уходом

    print('Creating dataset...')
    # Создадим пустой датасет - в зависимости от режима построения датасета - train или test
    if mode == 'train':
        dataset = sample.copy()[['user_id', 'is_churned', 'level', 'donate_total']]
    elif mode == 'test':
        dataset = sample.copy()[['user_id', 'level', 'donate_total']]

    # Пройдемся по всем источникам, содержащим "динамичекие" данные
    for df in [payments, reports, abusers, logins, pings, sessions, shop]:

        # Получим 'day_num_before_churn' для каждого из значений в источнике для определения недели
        #  - за сколько дней до чурна залогирован экшн
        data = pd.merge(sample[['user_id', 'login_last_dt']], df, on='user_id')
        data['day_num_before_churn'] = 1 + (data['login_last_dt'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d')) -
                                            data['log_dt'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))).apply(
            lambda x: x.days)
        df_features = data[['user_id']].drop_duplicates().reset_index(drop=True)  # UNIQUE DEVICE_ID

        # Для каждого признака создадим признаки для каждого из времененно интервала (в нашем примере 4 интервала по 7 дней)
        #
        features = list(set(data.columns) - set(['user_id', 'login_last_dt', 'log_dt', 'day_num_before_churn']))
        print('Processing with features:', features)
        for feature in features:
            for i, inter in enumerate(
                    inter_list):  # берем данные в нужный отрезок от даты чурна назад, считаем среднее по каждому девайсу
                inter_df = data.loc[data['day_num_before_churn'].between(inter[0], inter[1], inclusive=True)]. \
                    groupby('user_id')[feature].mean().reset_index(). \
                    rename(index=str, columns={feature: feature + '_{}'.format(i + 1)})
                df_features = pd.merge(df_features, inter_df, how='left', on='user_id')

        # Добавляем построенные признаки в датасет
        dataset = pd.merge(dataset, df_features, how='left', on='user_id')

        print(f'Run time (calculating features): {time_format(time.time() - start_t)}')

    # Добавляем "статические" признаки
    dataset = pd.merge(dataset, profiles, on='user_id')
    # ---------------------------------------------------------------------------------------------------------------------------
    dataset.to_csv(f'{dataset_path}dataset_raw_{mode}.csv', sep=';', index=False)
    print(
        f'Dataset is successfully built and saved to {dataset_path}, run time "build_dataset_raw": {time_format(time.time() - start_t)}')
