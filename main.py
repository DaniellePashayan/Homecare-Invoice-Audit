from tqdm import tqdm
import pandas as pd

import datetime as dt
from glob import glob
import numpy as np
import os
import pytz
import re

def convert_to_est(date_str):
    date = date_str.split(' ')
    date = ' '.join(date[0:3])

    # looks for 2 digit hour
    pattern = '(?<!\d)(\d{1})(?!\d)'
    match = re.search(pattern, date).group()
    if match:
        date = re.sub(pattern, r'0\g<1>', date)

    date_ist = dt.datetime.strptime(date, '%m/%d/%Y %I:%M:%S %p')
    ist_tz = pytz.timezone('Asia/Kolkata')
    date_ist = ist_tz.localize(date_ist)

    est_tz = pytz.timezone('US/Eastern')
    date_est = date_ist.astimezone(est_tz)

    return date_est

def combine(month:int, year: int, filter_month:int):
    str_month = str(month).zfill(2)
    str_year = str(year)

    columns = ['INVNUM', 'MRN', 'VisitNumber', 'Location', 'CodifyComments','Reason', 'RetrievalStatus','RetrievalDescription', 'CreatedDate', 'BOTRequestDate', 'LastModifiedDate','RecordAttemptCount']

    dtypes = {
        'INVNUM': 'int',
        'MRN': 'str',
        'VisitNumber': 'str',
        'Location': 'str',
        'CodifyComments': 'str',
        'Reason': 'str',
        'RetrievalDescription': 'str',
        'BotRequestDate': 'datetime64[ns]',
        'LastModifiedDate': 'datetime64[ns]'
    }
    
    path = '//NASHCN01/SHAREDATA/NewRefCenter/ANewReferralPHI/NS/BOT/Input & Output Files/'
    search_path = f'{path}*Outbound_{str_month}*{str_year}.xlsx'
    
    df = pd.concat([pd.read_excel(file, engine='openpyxl', dtype=dtypes) for file in glob(search_path) if '~' not in file])[columns]
    
    df = df.reset_index(drop=True)
    
    # gautam confirmed the report he uses for invoicing goes off CreatedDate
    df = df[df['CreatedDate'].dt.month == filter_month]
    
    # if CodifyComments is null, populate with value from "Reason" column
    df['CodifyComments'] = df['CodifyComments'].fillna(df['Reason'])
    return df

def parse_invoicing(outputs_df):
    # Create new columns for the extracted values
    outputs_df['CareportSuccessCount'] = outputs_df['RecordAttemptCount'].str.extract(r'\[(\d+)/\d+\]')
    outputs_df['CareportFailureCount'] = outputs_df['RecordAttemptCount'].str.extract(r'\[\d+/(\d+)\]')
    outputs_df['SunriseSuccessCount'] = outputs_df['RecordAttemptCount'].str.extract(r'\]\,\[(\d+)/\d+\]')
    outputs_df['SunriseFailureCount'] = outputs_df['RecordAttemptCount'].str.extract(r'\]\,\[\d+/(\d+)\]')

    # Convert the extracted values to numeric type
    outputs_df['CareportSuccessCount'] = pd.to_numeric(outputs_df['CareportSuccessCount'])
    outputs_df['CareportFailureCount'] = pd.to_numeric(outputs_df['CareportFailureCount'])
    outputs_df['SunriseSuccessCount'] = pd.to_numeric(outputs_df['SunriseSuccessCount'])
    outputs_df['SunriseFailureCount'] = pd.to_numeric(outputs_df['SunriseFailureCount'])

    nan_replacement = {
        'CareportSuccessCount': 0,
        'CareportFailureCount': 0,
        'SunriseSuccessCount': 0,
        'SunriseFailureCount': 0
    }

    # Replace NaN values in multiple columns
    outputs_df.fillna(nan_replacement, inplace=True)
    
    
    return outputs_df

def get_prior_month_year(month:int, year:int):
    # if month is january, return 12
    if month == 1:
        return 12, year - 1
    else:
        return month - 1, year

def get_next_month_year(month:int, year:int):
    # if month is december, return 1
    if month == 12:
        return 1, year + 1
    else:
        return month + 1, year

def categorize(df):
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

def main(month: int, year: int):
    prior_month, prior_year = get_prior_month_year(month, year)
    next_month, next_year = get_next_month_year(month, year)
    
    curr_month = combine(month,year,month)
    # since the bot is working overnight, sutherland charges based on the date the account is worked. at the end of the month, there are accounts that appear on the prior months inventory but get worked in the current month
    # as such, we must look at files for the prior month and parse out accounts that were worked in the current month
    last_month = combine(prior_month,prior_year,month)
    next_month_data = combine(next_month,next_year,month)

    
    files_to_combine = []
    if len(last_month) > 0:
        files_to_combine.append(last_month)
    if len(curr_month) > 0:
        files_to_combine.append(curr_month)
    if len(next_month_data) > 0:
        files_to_combine.append(next_month_data)
    
    files = pd.concat(files_to_combine)
    files = files.drop_duplicates()
    
    files_invoicing = parse_invoicing(files)
    files_invoicing = categorize(files_invoicing)
    
    # remove duplicate successes
    files_invoicing['CreatedDate'] = pd.to_datetime(files_invoicing['CreatedDate'])
    first_success = files_invoicing[files_invoicing['Reason'] == 'MR PDF Saved'].sort_values('CreatedDate').drop_duplicates('INVNUM')
    non_success = files_invoicing[files_invoicing['Reason'] != 'MR PDF Saved']
    final = pd.concat([first_success, non_success]).sort_values(by='CreatedDate').reset_index(drop=True)
    
    # at the start of a new year, a folder needs to be created for the year
    if not os.path.exists(f'//NT2KWB972SRV03/SHAREDATA/CPP-Data/CBO Westbury Managers/LEADERSHIP/Bot Folder/Part A/Home Care/Invoicing/{str(year)}'):
        os.mkdir(f'//NT2KWB972SRV03/SHAREDATA/CPP-Data/CBO Westbury Managers/LEADERSHIP/Bot Folder/Part A/Home Care/Invoicing/{str(year)}')
        
    file_path = f'//NT2KWB972SRV03/SHAREDATA/CPP-Data/CBO Westbury Managers/LEADERSHIP/Bot Folder/Part A/Home Care/Invoicing/{str(year)}/{str(month).zfill(2)} {str(year)}.xlsx'

    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        files_invoicing.to_excel(writer, sheet_name='Invoicing', index=None)
        final.to_excel(writer, sheet_name='Final', index=None)

if __name__ == '__main__':
    month = int(input('Enter month: '))
    year = int(input('Enter year: '))
    main(month, year)
    