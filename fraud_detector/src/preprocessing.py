import pandas as pd


def run_preproc(input_df):
    df = input_df.copy()
    time = pd.to_datetime(df.pop('transaction_time'))
    df['hour'] = time.dt.hour
    df['weekday'] = time.dt.dayofweek
    df['month'] = time.dt.month
    df['post_code'] = df['post_code'].astype(str)
    return df
