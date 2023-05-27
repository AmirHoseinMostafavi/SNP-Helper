# %%
import pandas as pd
import numpy as np
import os
from clickhouse_driver import Client
from datetime import date
import pyodbc
import enigma
import random
from minio import Minio
import os
# import shutup
# shutup.please()
import logging


# %%
def add_test_voucher(df_temp, group,id):
    if id not in df_temp['user_id'].values:
        df_temp.loc[len(df_temp)] = [id, group]     # For test Vouchers in VC
    return df_temp


# %%
def handle_datekey(date):
    date = str(date)
    return f'{date[:4]}-{date[4:6]}-{date[6:]}'

# %%

# Define the main function to create directories
def create_directories(camp_name: str, date: str, parent_dir: str):
    PARENT_DIR         = parent_dir
    RAW_DATA_DIR       = "Raw Data"
    FINAL_DATA_DIR     = "Final Data"
    EXCLUSION_DIR      = "Exclusion"
    HARD_EXCLUSION_DIR = "Hard"
    SOFT_EXCLUSION_DIR = "Soft"
    VOUCHERS_DIR       = "Vouchers"
    MYLIST_DIR         = "My Lists"
    
    camp_dir = os.path.join(PARENT_DIR, date)
    
    os.makedirs(camp_dir, exist_ok=True)
    os.makedirs(os.path.join(camp_dir, RAW_DATA_DIR), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, FINAL_DATA_DIR), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, RAW_DATA_DIR, camp_name), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, FINAL_DATA_DIR, camp_name), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, EXCLUSION_DIR), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, EXCLUSION_DIR, HARD_EXCLUSION_DIR), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, EXCLUSION_DIR, SOFT_EXCLUSION_DIR), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, EXCLUSION_DIR, MYLIST_DIR), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, EXCLUSION_DIR, MYLIST_DIR,camp_name), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, EXCLUSION_DIR, HARD_EXCLUSION_DIR, camp_name), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, EXCLUSION_DIR, SOFT_EXCLUSION_DIR, camp_name), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, VOUCHERS_DIR), exist_ok=True)
    os.makedirs(os.path.join(camp_dir, VOUCHERS_DIR, camp_name), exist_ok=True)
    
    logging.info(f"Directories for campaign {camp_name} on {date} have been created.")




# %%
def phone_type_plus98(m):
    m = str(m)
    if (len(m) < 10):
        return ''
    elif (m[0]=='9' and len(m) == 10):
        return f'+98{m}'
    elif m.startswith('09') and len(m) == 11:
        return f'+98{m[1:]}'
    elif m.startswith('98') and len(m) == 12:
        return f'+98{m[2:]}'
    elif m.startswith('+98') and len(m) == 13:
        return m
    else:
        return ''

def phone_type_09(m):
    m = str(m)
    if (len(m) < 10):
        return ''
    elif (m[0]=='9' and len(m)==10):
        return f'0{m}'
    elif m.startswith('09') and len(m) == 11:
        return m
    elif m.startswith('98') and len(m) == 12:
        return f'09{m[2:]}'
    elif m.startswith('+98') and len(m) == 13:
        return f'09{m[3:]}'
    else:
        return ''

def phone_type_9(m):
    m = str(m)
    if (len(m) < 10):
        return ''
    elif (m[0]=='9'and len(m) == 10):
        return m
    elif m.startswith('09') and len(m) == 11:
        return m[1:]
    elif m.startswith('98') and len(m) == 12:
        return f'{m[2:]}'
    elif m.startswith('+98') and len(m) == 13:
        return f'{m[3:]}'
    else:
        return ''
def phone_type_98(m):
    m = str(m)
    if (len(m) < 10):
        return ''
    elif (m[0]=='9'and len(m) == 10):
        return f'98{m}'
    elif m.startswith('09') and len(m) == 11:
        return f'98{m[1:]}'
    elif m.startswith('98') and len(m) == 12:
        return m
    elif m.startswith('+98') and len(m) == 13:
        return f'{m[1:]}'
    else:
        return ''


# %%

def hard_exclusion(camp_dir, df, camp_name_=""):
    df['phone'] = df['phone'].apply(lambda x: phone_type_9(x)).astype({'phone': str})
    exclusion_dir_Hard = f"{camp_dir}Hard/{camp_name_}"
    print(f"Original DataFrame shape: {df.shape}")
    dfs = []
    files = os.listdir(f"{exclusion_dir_Hard}")
    if not files:
        red_code = "\033[91m"
        reset_code = "\033[0m"
        message = 'No files in Hard Exclusion Folder!!'
        print(red_code + message + reset_code)
        return df
    else:
        for file in files:
            print(f"Processing file: {file}")
            if (".csv" in file):
                temp_df = pd.read_csv(
                    f"{exclusion_dir_Hard}/{file}",
                    dtype={"phone": str},
                )
            elif (".xlsx" in file):
                temp_df = pd.read_excel(
                    f"{exclusion_dir_Hard}/{file}",
                    dtype={"phone": str},
                )
            else:
                print(f"File format not supported: {file}")
                continue
            print(f"File has {temp_df.shape[1]} columns")
            if "phone" not in temp_df.columns:
                print(f"phone column not found in file: {file}")
                continue
            temp_df.phone = temp_df.phone.astype(str)
            dfs.append(temp_df[["phone"]])
    ex_df = pd.concat(dfs).drop_duplicates(subset=["phone"]).reset_index(drop=True)
    ex_df.phone = ex_df.phone.astype(str)
    ex_df.phone = ex_df.phone.apply(lambda x: phone_type_9(x)).astype(str)
    ex_df.phone = ex_df.phone.apply(lambda p: str(p).split(".")[0])
    df_res = df.set_index("phone").loc[list(set(df.phone) - set(ex_df.phone)), :].reset_index()
    print(f"After exclusion DataFrame shape: {df_res.shape}")
    return df_res


# %%

def blacklist_exclusion(camp_dir, df):
    df['phone'] = df['phone'].apply(lambda x: phone_type_9(x)).astype({'phone': str})
    exclusion_dir_Blacklist= f"{camp_dir}"
    print(f"Original DataFrame shape: {df.shape}")
    dfs = []
    files = os.listdir(f"{exclusion_dir_Blacklist}")
    if not files:
        red_code = "\033[91m"
        reset_code = "\033[0m"
        message = 'No files in Hard Exclusion Folder!!'
        print(red_code + message + reset_code)
        return df
    else:
        for file in files:
            print(f"Processing file: {file}")
            if (".csv" in file):
                temp_df = pd.read_csv(
                    f"{exclusion_dir_Blacklist}/{file}",
                    dtype={"phone": str},
                )
            elif (".xlsx" in file):
                temp_df = pd.read_excel(
                    f"{exclusion_dir_Blacklist}/{file}",
                    dtype={"phone": str},
                )
            else:
                print(f"File format not supported: {file}")
                continue
            print(f"File has {temp_df.shape[1]} columns")
            if "phone" not in temp_df.columns:
                print(f"phone column not found in file: {file}")
                continue
            temp_df.phone = temp_df.phone.astype(str)
            dfs.append(temp_df[["phone"]])
    ex_df = pd.concat(dfs).drop_duplicates(subset=["phone"]).reset_index(drop=True)
    ex_df.phone = ex_df.phone.astype(str)
    ex_df.phone = ex_df.phone.apply(lambda x: phone_type_9(x)).astype(str)
    ex_df.phone = ex_df.phone.apply(lambda p: str(p).split(".")[0])
    df_res = df.set_index("phone").loc[list(set(df.phone) - set(ex_df.phone)), :].reset_index()
    print(f"After exclusion DataFrame shape: {df_res.shape}")
    return df_res


# %%
def soft_exclusion(camp_dir, df, camp_name_=""):
    df['phone'] = df['phone'].apply(lambda x: phone_type_9(x)).astype({'phone':str})
    exclusion_dir_Soft  = f'{camp_dir}Soft\{camp_name_}'
    print(len(df))
    dfs = []
    if os.path.exists(f'{exclusion_dir_Soft}'):
        files = os.listdir(f'{exclusion_dir_Soft}')
        if len(files) > 0:
            for file in files:
                print(file)
                if ('.csv' in file):
                    temp_df = pd.read_csv(
                        f'{exclusion_dir_Soft}\{file}',
                        dtype={'phone': str},
                    )
                else:
                    temp_df = pd.read_excel(
                        f'{exclusion_dir_Soft}\{file}',
                        dtype={'phone': str},
                    )
                temp_df.phone = temp_df.phone.astype(str)
                temp_df.columns = ['phone']
                dfs.append(temp_df)
            ex_df = pd.concat(dfs).drop_duplicates(subset=['phone']).reset_index(drop=True)
            ex_df.phone = ex_df.phone.astype(str)
            ex_df.phone = ex_df.phone.apply(lambda x : phone_type_9(x)).astype(str)
            ex_df.phone = ex_df.phone.apply(lambda p:str(p).split('.')[0])
            df['Launch Date'] = 'Next Day'
            set_phone_df = set(df.phone)
            df = df.set_index('phone')
            df.loc[list(set_phone_df-set(ex_df.phone)),'Launch Date'] = 'Today'
            print(df['Launch Date'].value_counts())
            return df.reset_index()
    df['Launch Date'] = 'Today'
    return df


# %%
def read_file(camp_dir):
    for file in os.listdir(camp_dir):
        file_path = os.path.join(camp_dir, file)
        if file.endswith('.xlsx'):
            df = pd.read_excel(file_path)
            print(f'file name is :  {file} and length is :  {len(df)}')
        elif file.endswith('.csv'):
            df = pd.read_csv(file_path)
            print(f'file name is :  {file} and length is :  {len(df)}')
    return df , file_path

# %%
def segment_customers(cg_pcnt, df):
    CG = random.sample([*df.index.values],k=int(len(df)*cg_pcnt))
    final_raw_indices = [*{*df.index.values} - set(CG)]
    df['Group'] = df.index.values
    df.loc[CG, 'Group'] = 'CG'
    df.loc[final_raw_indices, 'Group'] = 'A'
    return df

# %%
def save_file(df, file_path):
    if '.csv' in file_path:
        df.to_csv(file_path, index = False)
    else:
        df.to_excel(file_path, index = False)


# %%
def process_vouchers(camp_dir):
    with open(camp_dir, "r") as f:
        Voucher_code_text = f.read()

    voucher_list = Voucher_code_text.split("\n")

    # Create an empty list to store the data
    data = []

    # Iterate over the vouchers
    for voucher in voucher_list:
        voucher = voucher.strip()
        if voucher == "":
            continue
        try:
            name, details = voucher.split("==>")
        except ValueError:
            continue
        value, details = details.split("Toman Voucher")
        service, details = details.split("(")
        if ' AND ' in details:
            service, details = details.split(" AND ")
        details = details.strip("):")
        if "Not First Use" in details:
            first_use = "0"
        elif "First Use" in details:
            first_use = "1"
        if "Min Basket" in details:
            min_basket, details = details.split("Toman")
            min_basket = min_basket.strip()
            min_basket = min_basket.split(" ")[-1]
        else:
            min_basket = ""
        if "Vouchers:" in details:
            volume, details = details.split("Vouchers:")
            volume = volume.strip()
            volume = volume.split(" ")[-1]
        else:
            volume = ""
        expiration = details.strip()
        expiration = pd.to_datetime(expiration.replace("Expiration ", ""), format='%Y-%b-%d')

        # Append the data to the list
        data.append([name, value[:-1].strip(), service, first_use, min_basket, volume, expiration])

    # Create the dataframe
    df = pd.DataFrame(data, columns=["Name", "Value", "Service", "First Use", "Min Basket", "Volume", "Expiration"])
    return df


# %%


# %%
# phone column must be rename to column to work
def create_combine_file(camp_dir):
    dfs = []
    files = os.listdir(camp_dir)
    for file in files:
        print(file)
        if ('.csv' in file):
            temp_df = pd.read_csv(camp_dir+file, dtype={'phone':str})
        else:
            temp_df = pd.read_excel(camp_dir+file, dtype={'phone':str})
        temp_df.rename(columns={'phone': 'phone'})
        temp_df.phone = temp_df.phone.astype(str)
        print(len(temp_df))
        # temp_df.columns = ['phone']
        dfs.append(temp_df)
    ex_df = pd.concat(dfs).reset_index(drop=True)
    ex_df.phone = ex_df.phone.astype(str)
    ex_df.phone = ex_df.phone.apply(lambda x : phone_type_9(x)).astype(str)
    ex_df.phone = ex_df.phone.apply(lambda p:str(p).split('.')[0])
    print(len(ex_df))
    return ex_df


# %%
def handle_city_name(city):
    city_map = {
        'thr': 'TEHRAN', 'THR': 'TEHRAN', 'tehran': 'TEHRAN','تهران':'TEHRAN','تجریش':'TEHRAN','شهر تهران':'TEHRAN',
        'mas': 'MASHHAD', 'MAS': 'MASHHAD', 'mashhad': 'MASHHAD','مشهد':'MASHHAD','شهر مشهد':'MASHHAD',
        'isf': 'ISFAHAN', 'ISF': 'ISFAHAN', 'isfahan': 'ISFAHAN','اصفهان':'ISFAHAN','شهر اصفهان':'ISFAHAN',
        'kar': 'KARAJ', 'KAR': 'KARAJ', 'karaj': 'KARAJ','کرج': 'KARAJ','شهر کرج': 'KARAJ',
        'ahw': 'AHWAZ', 'AHW': 'AHWAZ', 'ahwaz': 'AHWAZ','اهواز': 'AHWAZ','شهر اهواز': 'AHWAZ',
        'tab': 'TABRIZ', 'TAB': 'TABRIZ', 'tabriz': 'TABRIZ','تبریز': 'TABRIZ',
        'shiraz': 'SHIRAZ', 'shi': 'SHIRAZ', 'SHI': 'SHIRAZ','شیراز': 'SHIRAZ','شهر شیراز': 'SHIRAZ',
        'qom': 'QOM','قم': 'QOM','شهر قم': 'QOM',
        'urm': 'URMIA', 'URM': 'URMIA', 'urmia': 'URMIA','ارومیه':'URMIA','شهر ارومیه':'URMIA',
        'gil': 'RASHT', 'GIL': 'RASHT', 'rasht': 'RASHT','رشت': 'RASHT','شهر رضا': 'RASHT'
    }
    if city in city_map:
        return city_map[city]
    else:
        return None



