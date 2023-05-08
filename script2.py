import pandas as pd
import glob
import os
import re
import numpy as np
from datetime import datetime


def parse_date(date_str: str):
    if not isinstance(date_str, str):
        date_str = str(date_str)
    parsed = pd.to_datetime(date_str, errors='coerce')
    if not pd.isnull(parsed):
        return parsed
    else:
        find_s = re.match(r'(\d+)\s(\d+:)', date_str)
        if find_s:
            date_obj = datetime.strptime(find_s.group(1), '%d%m%Y')
            s = str(date_obj.year) + '-' + str(date_obj.month) + '-' + str(
                date_obj.day)
            res = date_str.replace(find_s.group(1), s)
            return pd.to_datetime(res, errors='coerce')
        elif re.findall(r'(\d+:\d+:\d+)', date_str) and re.findall(
                r'(\D+)', date_str):
            date_str = date_str.replace('/', '')
            return pd.to_datetime(
                date_str, format='%d%m%Y %H:%M:%S %p', errors='coerce')


def read_files(path: str):
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    df = df.fillna(value=np.nan).replace('na', np.nan)
    df['player_id'] = df['player_id'].fillna(0).astype(int)
    if os.path.basename(path) == 'payments':
        df['paid_amount'] = df['paid_amount'].apply(
            pd.to_numeric, errors='coerce')
        df['Date'] = df['Date'].apply(parse_date)
        df = df[df['Date'].notna()]
    elif os.path.basename(path) == 'bets':
        df['amount'] = df['amount'].apply(pd.to_numeric, errors='coerce')
        df['accept_time'] = df['accept_time'].apply(parse_date)
        df = df[df['accept_time'].notna()]
    return df.reset_index(drop=True)


def date_diff(start_date: datetime, end_date: datetime):
    return (start_date - end_date) / pd.Timedelta(minutes=1)


def create_directory(path: str):
    is_directory = os.path.exists(path)
    if not is_directory:
        os.makedirs(path)


def rank_data(lst: list):
    res = list(sorted(lst, key=lambda x: (x[0], x[1])))
    counter = 1
    for i in range(1, len(res)):
        if res[i][2] == res[i - 1][2] and res[i][0] == \
                res[i - 1][0]:
            if res[i][3] > 1.5 and res[i - 1][3] > 1.5:
                counter += 1
        else:
            counter = 1
        res[i].append(counter)
    return res


def run():
    paths = ["./payments", "./bets"]
    frames = list(map(read_files, paths))

    dep = pd.merge(frames[0], frames[1], on='player_id', how='inner')
    dep['p_b_diff'] = np.vectorize(date_diff)(dep['accept_time'], dep['Date'])
    dep = dep[(dep['p_b_diff'] <= 60) & (dep['p_b_diff'] >= 0)]
    dep = dep[(dep['paid_amount'] > dep['amount'] * 0.9)
              & (dep['paid_amount'] <= dep['amount'] * 1.1)]
    withdrawal = frames[0][frames[0]['transaction_type'] == 'withdrawal']
    result = pd.merge(dep, withdrawal[['Date', 'player_id', 'payment_method_name']],
                      on='player_id', how='inner')

    result['min_diff'] = (result.Date_y - result.Date_x) / pd.Timedelta(minutes=1)
    result = result[(result['min_diff'] <= 60) & (result['min_diff'] >= 0)]
    result = result[(result['payment_method_name_x'] != result['payment_method_name_y'])]

    create_directory('result')
    dt_string = datetime.now().strftime("%S%M%H%d%m%Y")
    result.to_csv(f'result/result{dt_string}.csv')

    bets = frames[1]
    bets = bets.reset_index(drop=True)

    clients = list(zip(
        bets['player_id'].tolist(),
        bets['accept_time'].tolist(),
        bets['result'].tolist(),
        bets['payout'].tolist(),
        ))

    sorted_bets = list(
        sorted(list(map(list, clients)), key=lambda x: (x[0], x[1])))
    df_bets = pd.DataFrame(rank_data(sorted_bets), columns=[
        'player_id',
        'accept_time',
        'result',
        'payout',
        'rank_numb'])
    df_wins = df_bets[(df_bets['rank_numb'] >= 5)]
    df_wins.to_csv(f'result/bets_result{dt_string}.csv')
