import pandas as pd
from modules.dataset_builder import build_dataset_raw
from utils.preprocessing import prepare_dataset

CHURNED_START_DATE = '2019-09-01'
CHURNED_END_DATE = '2019-10-01'

INTER_1 = (1, 3)
INTER_2 = (4, 10)
INTER_3 = (11, 17)
INTER_4 = (18, 24)
INTER_5 = (25, 31)
INTER_LIST = [INTER_1, INTER_2, INTER_3, INTER_4, INTER_5]


def build_df(MODE, RAW_DATA_PATH, DATASET_PATH):
    X = None
    y = None

    # build_dataset_raw(churned_start_date=CHURNED_START_DATE,
    #                   churned_end_date=CHURNED_END_DATE,
    #                   inter_list=INTER_LIST,
    #                   raw_data_path=RAW_DATA_PATH,
    #                   mode=MODE)

    df = pd.read_csv(f'{DATASET_PATH}/dataset_raw_{MODE}.csv', sep=';', index_col=0)

    prepare_dataset(dataset=df, dataset_path=f'{DATASET_PATH}', dataset_type=MODE, inter_list=INTER_LIST)

    df = pd.read_csv(f'{DATASET_PATH}dataset_{MODE}.csv', sep=';', index_col=0)
    if MODE == 'train':
        X = df.drop(['is_churned'], axis=1)
        y = df['is_churned']
    elif MODE == 'test':
        X = df

    return X, y


