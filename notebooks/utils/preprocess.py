import datetime as dt
import pandas as pd



def make_etz_time(df: pd.DataFrame, utc_col:str='utc_time', new_col:str='etz_time', time_diff = -5):
    df[new_col] = df[utc_col] + dt.timedelta(hours=time_diff)
    return df

def make_daily_ohlcvs(hourly_df: pd.DataFrame):
    """ 
    TODO: Need to change for CryptoCompare dataset!
    """
    hourly_df['etz_date'] = hourly_df['etz_time'].apply(lambda x: x.date())
    conditions = {
            "etz_time": "first",
            "opening_price": "first",
            "high_price": "max", 
            "low_price": "min",  
            "trade_price": "last",
            "candle_acc_trade_price": "sum",
            "candle_acc_trade_volume": "sum"
    }
    daily_df = hourly_df.groupby('etz_date').agg(conditions)
    return daily_df
