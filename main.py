import pandas as pd
from datetime import datetime
import os

def read_transaction_report(month: str, year: str) -> pd.DataFrame:
    columns = ['INVNUM', 'MRN', 'VisitNumber', 'Location', 'CodifyComments','Reason', 'RetrievalStatus','RetrievalDescription', 'CreatedDate', 'BOTRequestDate', 'LastModifiedDate','RecordAttemptCount', 'BotName']

    dtypes = {
        'INVNUM': 'str', # since this contains all the bots, need to read as string and then convert later
        'MRN': 'str',
        'VisitNumber': 'str',
        'Location': 'str',
        'CodifyComments': 'str',
        'Reason': 'str',
        'RetrievalDescription': 'str',
        'BotRequestDate': 'datetime64[ns]',
        'LastModifiedDate': 'datetime64[ns]'
    }

    transaction_report_path = f'//NT2KWB972SRV03/SHAREDATA/CPP-Data/Sutherland RPA/Northwell Process Automation ETM Files/Monthly Reports/.Sutherland Reports/{year}/{year} {month} - Transaction Report.xlsx'

    data = pd.read_excel(transaction_report_path, sheet_name='export', dtype=dtypes, usecols=columns)

    return data[data['BotName'] == 'HomeCareDischarge']

def parse_attempt_count(df: pd.DataFrame) -> pd.DataFrame:
    # Create new columns for the extracted values
    df['CareportSuccessCount'] = df['RecordAttemptCount'].str.extract(r'\[(\d+)/\d+\]')
    df['CareportFailureCount'] = df['RecordAttemptCount'].str.extract(r'\[\d+/(\d+)\]')
    df['SunriseSuccessCount'] = df['RecordAttemptCount'].str.extract(r'\]\,\[(\d+)/\d+\]')
    df['SunriseFailureCount'] = df['RecordAttemptCount'].str.extract(r'\]\,\[\d+/(\d+)\]')

    # Convert the extracted values to numeric type
    df['CareportSuccessCount'] = pd.to_numeric(df['CareportSuccessCount'])
    df['CareportFailureCount'] = pd.to_numeric(df['CareportFailureCount'])
    df['SunriseSuccessCount'] = pd.to_numeric(df['SunriseSuccessCount'])
    df['SunriseFailureCount'] = pd.to_numeric(df['SunriseFailureCount'])

    nan_replacement = {
        'CareportSuccessCount': 0,
        'CareportFailureCount': 0,
        'SunriseSuccessCount': 0,
        'SunriseFailureCount': 0
    }

    # Replace NaN values in multiple columns
    df.fillna(nan_replacement, inplace=True)
    
    return df

def categorize(df: pd.DataFrame) -> pd.DataFrame:
    # based on the reason column
    categories = {
        "Response Reason is not 'Yes'": "Response Reason Not Matched",
        'Visit Status': "Visit Status Not Matched",
        'MR PDF Saved': "MR PDF Saved",
        'Documents do not match criteria': "No Documents Found",
        'Referral Number in Patient Info header': "Referral ID Validation Failed",
        'Patient Information': "No Patient Info Found",
        'Visit Type': "Visit Type Not Matched"
    }
    
    df['Reason'] = df['Reason'].map(categories)
    return df

def create_pivots(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    
    careport_success = df.pivot_table(index='Reason', columns='CareportSuccessCount', values='INVNUM', aggfunc='count', fill_value=0)
    sunrise_success = df.pivot_table(index='Reason', columns='SunriseSuccessCount', values='INVNUM', aggfunc='count', fill_value=0)
    
    return careport_success, sunrise_success

if __name__ == '__main__':
    month = str(datetime.now().month - 1).zfill(2)
    if month == '00':
        month = '12'
        year = str(datetime.now().year - 1)
    else:
        year = str(datetime.now().year)
    
    df = read_transaction_report(month, year)
    df = parse_attempt_count(df)
    df = categorize(df)
    
    careport_success, sunrise_success = create_pivots(df)
    folder = f'//NT2KWB972SRV03/SHAREDATA/CPP-Data/CBO Westbury Managers/LEADERSHIP/Bot Folder/Part A/Home Care/Invoicing/{year}/'
    os.makedirs(folder, exist_ok=True)
    with pd.ExcelWriter(f'//NT2KWB972SRV03/SHAREDATA/CPP-Data/CBO Westbury Managers/LEADERSHIP/Bot Folder/Part A/Home Care/Invoicing/{year}/{month} {year}.xlsx') as writer:
        df.to_excel(writer, sheet_name='Data')
        careport_success.to_excel(writer, sheet_name='Careport Success')
        sunrise_success.to_excel(writer, sheet_name='Sunrise Success')
    