import csv
from datetime import date

import pandas as pd
import urllib3
import yfinance as yf


import SSHConn


def asx_date():
    end = date.today()
    start = date(end.year - 3, end.month, end.day)

    return start, end


def get_asx_companies_df(today):
    href = "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file?access_token=83ff96335c2d45a094df02a206a39ff4"
    http = urllib3.PoolManager()
    response = http.request('GET', href)

    decoded_data = response.data.decode('utf-8')

    cr = csv.reader(decoded_data.splitlines(), delimiter=',')
    asx_companies = list(cr)
    df = pd.DataFrame(asx_companies)

    # set columns to first row names
    df.columns = df.iloc[0]
    df = df[1:]

    df['yfinance ASX code'] = df['ASX code'] + '.AX'

    # get db engine and write df to db
    engine = SSHConn.mysql_connect()
    df.to_sql(con=engine, name='asx_dataframe1', if_exists='replace')

    # write df to csv file
    # filename = "asx-listed-comps-" + str(today) + ".csv"
    # df.to_csv(filename)
    return df


def explore_listed_comp_data(df):
    # how many in each industry group
    print(df['GICs industry group'].value_counts())
    print(df['GICs industry group'].unique())


def get_comp_asx_code(df, industry):
    # select companies based on industry
    df_new = df.loc[df['GICs industry group'] == industry]

    ticker_list = df_new['yfinance ASX code'].to_list()
    return ticker_list


def get_yfinance_data(tl, start_date, end_date, file_name):
    df = yf.download(tl, period='1d', start=start_date, end=end_date)
    print(df)
    df.to_csv(file_name)


def get_yfinance_data_tickers(tl, start_date, end_date, file_name):
    ticker_data = yf.Tickers(tl)
    print(ticker_data)


if __name__ == "__main__":

    start, end = asx_date()

    asx_df = get_asx_companies_df(end)


    # explore_listed_comp_data(asx_df)
    # Look at companies under 'Bank'
    # asx_codes = get_comp_asx_code(asx_df, industry='Banks')

    # filename = "asx-data-" + str(end) + ".csv"
    # get_yfinance_data(asx_codes, start, end, filename)


