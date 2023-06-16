import sqlite3
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')
import traceback
import os

filepath = "D:\\Master Source Files\\May 2023\\"
filenames = os.listdir(filepath)
categories_df = pd.read_excel('account_categories.xlsx')
main_df = pd.DataFrame()

for filename in tqdm(filenames):
    df_files = pd.read_excel(filepath + filename, sheet_name=None)
    if len(df_files) == 2:
        sheet_names = list(df_files.keys())
        df1 = df_files[sheet_names[0]]
        df2 = df_files[sheet_names[1]]
        col_to_row = df2.columns.values.tolist()
        col_df = pd.DataFrame([col_to_row], columns=df1.columns)
        df2.columns = df1.columns
        df = pd.concat([df1, col_df, df2])
    elif len(df_files) == 1:
        df = pd.read_excel(filepath + filename)
    
    month_num = int(df.iloc[1].values[0].split(' ')[-1].split('-')[1])
    year_num = int(df.iloc[1].values[0].split(' ')[-1].split('-')[2].split(")")[0])
    month_date = str(month_num) + "-" + "1" + "-" + str(year_num)
    
    branch_num = int(df.iloc[2].values[0].split('-')[0].split(':')[-1])
    
    columns = ['No', 'CIF', 'NRC', 'Address', 'Phone', 'Account_Number', 'Account_Name', 'Interest_Rate(%)',
               'Minimum_Balance', 'Status', 'Open_Date', 'Stock_No', 'Period', 'Begin_Tenor_Date', 'End_Tenor_date',
               'Available_Balance_FC', 'Available_Balance_Equivalent', 'Balance_FC', 'Balance_Equivalent',
               'Rollover_Option']
    
    df.columns = columns
    df = df.iloc[8:]
    df.reset_index(drop=True, inplace=True)
    
    subtotals = df[df['No'] == 'SubTotal By : ']
    subtotals.reset_index(inplace=True)
    subtotals_list = subtotals[['index', 'Address']].rename(columns={'index': 'index', 'Address': 'subcategory'})
    
    branch_df = pd.DataFrame()
    start_index = 0
    for item in subtotals_list.itertuples(index=False):
        end_index = item[0]
        subcategory = item[1]
        category = categories_df.loc[categories_df['SubCategory'] == subcategory, 'Category'].iloc[0]
        dummy_df = df[start_index: end_index]
        dummy_df['SubCategory'] = subcategory
        dummy_df['Category'] = category
        dummy_df['BranchCode'] = branch_num
        dummy_df['Month_Date'] = month_num
        branch_df = pd.concat([branch_df, dummy_df])
        start_index = end_index + 1
        
    main_df = pd.concat([main_df, branch_df])

main_df.dropna(subset=['CIF'], inplace=True)

outname = input("Enter output filename: ")
main_df.to_csv(outname+'.csv', index=False)

#################### Insert Data into Data Mart ##########################

df = main_df

df_status = pd.read_csv("Status.csv")
df_product = pd.read_csv("Product.csv")

# Connect or create a database
conn = sqlite3.connect('test_db5.sqlite')
conn.execute('PRAGMA journal_mode = WAL')
cur = conn.cursor()

try:
    cur.executescript("""
    DROP TABLE IF EXISTS CIF;
    DROP TABLE IF EXISTS Master;

    CREATE TABLE CIF (
        id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        cif INTEGER NOT NULL UNIQUE,
        account_name TEXT
    );

    CREATE TABLE Master (
        id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        cif_id INTEGER,
        account_number INTEGER NOT NULL UNIQUE,
        balance FLOAT,
        product_id INTEGER,
        month INTEGER,
        branch INTEGER,
        status_id INTEGER
    );
    """)

    # Begin transaction
    cur.execute('BEGIN TRANSACTION')

    count = 0
    data = []
    for index, row in tqdm(df.iterrows()):
        cif = row['CIF']
        account_name = row['Account_Name']
        account_number = int(row['Account_Number'])
        balance = row["Available_Balance_Equivalent"]
        month = row["Month_Num"]
        branch = row["BranchCode"]

        status = row['Status']
        status_id = int(df_status[df_status["status"] == status].iloc[0][0])

        subcategory = row['SubCategory']
        product_id = int(df_product[df_product["product"] == subcategory].iloc[0][0])

        data.append((cif, account_name, cif, account_number, balance, product_id, month, branch, status_id))

        count += 1
        if count % 50000 == 0:
            cur.executemany('INSERT OR IGNORE INTO CIF (cif, account_name) VALUES (?, ?)', data)
            cur.executemany('INSERT OR REPLACE INTO Master (cif_id, account_number, balance, product_id, month, branch, status_id) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
            data = []

    # Insert any remaining rows
    if data:
        cur.executemany('INSERT OR IGNORE INTO CIF (cif, account_name) VALUES (?, ?)', data)
        cur.executemany('INSERT OR REPLACE INTO Master (cif_id, account_number, balance, product_id, month, branch, status_id) VALUES (?, ?, ?, ?, ?, ?, ?)', data)

    # Commit transaction
    cur.execute('COMMIT')

except Exception as e:
    print("The index of the row that caused the error is: ", index)
    print('Error occurred -', e)
    print(traceback.format_exc())
    # Revert changes because of exception
    conn.rollback()

finally:
    cur.close()
    conn.close()
