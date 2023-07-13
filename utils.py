from re import sub
from datetime import datetime


def camel_case(s):
    """rewrite string in camelCase format"""
    s = sub(r'[_-]+', ' ', s).title().replace(' ', '')
    return ''.join([s[0].lower(), s[1:]])


def write_parquet(df):
    """write df to parquet file"""
    now = datetime.now()
    dt_string = now.strftime('%d%m%Y%H%M%S')
    df.write.parquet(f'output/{dt_string}.parquet')
