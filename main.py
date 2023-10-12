import pandas as pd
import numpy as np
import os
import warnings
import sys

warnings.filterwarnings('ignore')
from dillards_utils import ETL_Client
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from datetime import datetime, timedelta
import time

t = ETL_Client()
d = ETL_Client(whse="db2")  

user = 'altunty'
buyer_ln = 'turker'
request_id = f"{user}_{buyer_ln.lower()}_{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}"
group = 'scortusr1'
data_path = f'/cfsshares/prod/parquet/{group}/altunty/or_utils/order_projection'
orders_table = 'DDMSCA.SCA_ORC_ORDERS'
table_name = 'SCSOR_DB_LAB.order_projection_recomended_order_units'
missing_sku_table = 'SCSOR_DB_LAB.order_projection_missing_skus'
start_time = time.time()

# Read the CSV file into a DataFrame and set data types as strings
input_df = pd.read_csv('/home/scortusr1/scripts/altunty/NEW_PROJECT/input.csv', dtype=str)

# Convert the 'coverage_day' column values to integers
input_df['coverage_day'] = input_df['coverage_day'].astype(int)

# Rename the 'first_ship_date' column to 'provided_date'
input_df.rename(columns={'first_ship_date':'provided_date'}, inplace=True)

# Display the modified DataFrame
input_df

#Empty Cell Checking and Dropping duplicates 
if input_df.isnull().any().any():
    print("WARNING: There are missing values in the dataset. Please review the data and ensure all fields are complete.")
    
else:
    print ("No missing values detected.")
    
# Find duplicate rows in the DataFrame
input_duplicates = input_df[input_df.duplicated()]

if not input_duplicates.empty:
    print ("Please check your input. Duplicate values found:")
    print(input_duplicates)
else:
    print("No duplicates found.")
    
print('Searching skus for the pairs of style & color') 

# Extract unique pairs of 'style' and 'color' from the DataFrame
unique_pairs = input_df.drop_duplicates(subset=['style', 'color'])[['style', 'color']].values.tolist()

# Generate a list of SQL conditions based on the unique style-color pairs
conditions = [f"(STYLE = '{pair[0]}' AND COLOR = '{pair[1]}')" for pair in unique_pairs]

# Join the conditions with 'OR' to create a single SQL WHERE clause
condition_forSQL = " OR ".join(conditions)

f_name = ("/home/scortusr1/scripts/altunty/NEW_PROJECT/sql_file/sku.sql")
with open (f_name, "r") as sql_fn:
        sql_file = sql_fn.read()
        sql_file = sql_file.replace("{condition_forSQL}", condition_forSQL )
        sql_file = sql_file.replace('"','')
        sql_file = sql_file.replace("',)","')")
        
# Define the temporary path for storing parquet files
tmp_path = ("/home/scortusr1/scripts/altunty/NEW_PROJECT/path/sku")

# Ensure the directory exists; if not, create it
os.makedirs(tmp_path, exist_ok=True)

# Fetch data using the 'fetch_data' function with the provided SQL file, and then write the results to a parquet file, overwriting if it exists
t.fetch_data(sql_file).write.parquet(tmp_path, mode="overwrite")

# Read the parquet file into a pandas DataFrame
sku_df = pd.read_parquet(tmp_path)     #Data frame of skus belonging to color and style pairs

# Merge 'sku_df' and 'input_df' DataFrames based on the columns 'style' and 'color' using an outer join
input_sku_df = pd.merge(sku_df,input_df,on=['style', 'color'], how='outer')

## Get unique SKUs from the 'sku' column of input_sku_df
sku_list= str(tuple(input_sku_df['sku'].unique()))

# Getting data from ddmsca.sca_orc_orders
print(f' Getting orders data from {orders_table} for warehouse')


f_name = ("/home/scortusr1/scripts/altunty/NEW_PROJECT/sql_file/whse_sku_orders_df.sql")
with open (f_name, "r") as sql_fn:
        sql_file = sql_fn.read()
        sql_file = sql_file.replace("{sku_list}", sku_list)
        sql_file = sql_file.replace('"','')
        sql_file = sql_file.replace("',)","')")

# Define the temporary path for storing parquet files related to orders
tmp_path = ("/home/scortusr1/scripts/altunty/NEW_PROJECT/path/whse_sku_orders")

# Ensure the directory exists; if not, create it
os.makedirs(tmp_path, exist_ok=True)

# Fetch data using the 'fetch_data' function with the provided SQL file, and then write the results to a parquet file, overwriting if it exists
t.fetch_data(sql_file).write.parquet(tmp_path, mode="overwrite")

# Read the parquet file into a pandas DataFrame
whse_sku_orders_df = pd.read_parquet(tmp_path)

# Rename the 'store' column to 'input_source_store'
whse_sku_orders_df.rename(columns={"store":"input_source_store"}, inplace=True)
        
        
 # Check for duplicates
if whse_sku_orders_df.duplicated().any():
    print("Alert: The system has detected duplicate rows in the DataFrame.")
    print("For data integrity, the program will now terminate. Please address the duplicates and try again.")
    sys.exit()  # Exit the program
else:
    print(" No duplicate rows detected for warehouse orders.")
    
#To calculate store short term oo , the sku are looked for in ddmsca.sca_orc_order.

f_name = ("/home/scortusr1/scripts/altunty/NEW_PROJECT/sql_file/store_sku_orders_df.sql")
with open (f_name, "r") as sql_fn:
        sql_file = sql_fn.read()
        sql_file = sql_file.replace("{sku_list}", sku_list)
        sql_file = sql_file.replace('"','')
        sql_file = sql_file.replace("',)","')")

# Define the temporary path for storing parquet files related to orders
tmp_path = ("/home/scortusr1/scripts/altunty/NEW_PROJECT/path/store_sku_orders")

# Ensure the directory exists; if not, create it
os.makedirs(tmp_path, exist_ok=True)

# Fetch data using the 'fetch_data' function with the provided SQL file, and then write the results to a parquet file, overwriting if it exists
t.fetch_data(sql_file).write.parquet(tmp_path, mode="overwrite")

# Read the parquet file into a pandas DataFrame
store_sku_orders_df = pd.read_parquet(tmp_path)

#Check for duplicates
if store_sku_orders_df.duplicated().any():
    print("Alert: The system has detected duplicate rows in the DataFrame.")
    print("For data integrity, the program will now terminate. Please address the duplicates and try again.")
    sys.exit()  # Exit the program
else:
    print("No duplicate rows detected for store orders.")       

print("Collecting DB2 on hand information")

#Getting real time data from DB2 (store and input_source_store on hand info,parent_sku,baby_sku,order_level etc)

f_name = ("/home/scortusr1/scripts/altunty/NEW_PROJECT/sql_file/data.sql")
 
with open(f_name,"r") as sql_fn:
    sql_file = sql_fn.read()
    sql_file = sql_file.replace("{sku_list}",sku_list)
    sql_file = sql_file.replace('"','')
    sql_file = sql_file.replace("',)","')")
    
#print(sql_file)

# Define the temporary path for storing parquet files related to real data
tmp_path = "/home/scortusr1/scripts/altunty/NEW_PROJECT/path/realdata/"

# Ensure the directory exists; if not, create it
os.makedirs(tmp_path, exist_ok=True)

# Fetch data using the 'fetch_data' function with the provided SQL file, and then write the results to a parquet file, overwriting if it exists
d.fetch_data(sql_file).write.parquet(tmp_path, mode="overwrite")

# Read the parquet file into a pandas DataFrame
on_hand_df_db2 = pd.read_parquet(tmp_path)
on_hand_df_db2.drop(['lead_time'],axis=1,inplace=True)

#Group by input_source_store and get the number of unique stores
num_of_store = on_hand_df_db2.groupby('input_source_store')['store'].nunique().reset_index()
num_of_store = num_of_store.rename(columns={'store': 'num_of_store'})

# Merge this with the original DataFrame to get the desired output
on_hand_df_db2 = on_hand_df_db2.merge(num_of_store, on='input_source_store', how='left')

# Find the skus that are not available in DB2.There is no any active store for that sku.

sku_list = input_sku_df['sku'].unique()

sku_on_hand_list = on_hand_df_db2['parent_sku'].unique()

missing_skus = [sku for sku in sku_list if sku not in sku_on_hand_list]  
  
# create warehouse on hand dataframe to have whse_slu+level data
whse_on_hand_df = on_hand_df_db2.groupby(['input_source_store','parent_sku','baby_sku','parent_oh','baby_oh']).first().reset_index()
whse_on_hand_df = whse_on_hand_df[['input_source_store','parent_sku','baby_sku','parent_oh','baby_oh','ppk_qty','lt_days','num_of_store']] 

#join whse on hand data and  whse orders 

whse_onhand_orders = pd.merge(whse_on_hand_df,whse_sku_orders_df, 
                                           left_on=['parent_sku','input_source_store'],
                                           right_on=['sku','input_source_store'],
                                           how='left')

# Remove columns ending with '_y'
whse_onhand_orders = whse_onhand_orders.drop([col for col in whse_onhand_orders if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
whse_onhand_orders.columns = [col.replace('_x', '') for col in whse_onhand_orders.columns]

#fill the Nan columns with correct values

whse_onhand_orders['item_id'] = whse_onhand_orders['parent_sku'].map(input_sku_df.set_index('sku')['item_id'])
whse_onhand_orders['style'] = whse_onhand_orders['parent_sku'].map(input_sku_df.set_index('sku')['style'])
whse_onhand_orders['color'] = whse_onhand_orders['parent_sku'].map(input_sku_df.set_index('sku')['color'])
whse_onhand_orders['sku'] = whse_onhand_orders['parent_sku']


# to have style.color,provided_date,coverage date join with input_sku_df
sku_whse_oh_order = pd.merge(whse_onhand_orders,input_sku_df, 
                                           left_on=['sku','style','color'],
                                           right_on=['sku','style','color'],
                                           how='inner')
# Remove columns ending with '_y'
sku_whse_oh_order = sku_whse_oh_order.drop([col for col in sku_whse_oh_order if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
sku_whse_oh_order.columns = [col.replace('_x', '') for col in sku_whse_oh_order.columns]

# Calculating of short_term_oo_from_vndr for whse  only for sku that in whse_orders


# Convert the 'first_ship_date' and 'transtit_date' column to a datetime format and then format it to 'YYYY-MM-DD' string format
sku_whse_oh_order['first_ship_date'] = pd.to_datetime(sku_whse_oh_order['first_ship_date']).dt.strftime('%Y-%m-%d')
sku_whse_oh_order['transit_date'] = pd.to_datetime(sku_whse_oh_order['transit_date']).dt.strftime('%Y-%m-%d')

# Compute the 'received_date' by adding the 'lt_days' (lead time days) to the 'provided_date' and store the result in the 'received_date' column
sku_whse_oh_order['received_date'] = pd.to_datetime(sku_whse_oh_order['provided_date']) + pd.to_timedelta(sku_whse_oh_order['lt_days'], unit='D')

# Compute the 'coverage_date' by adding the 'coverage_day' to the 'received_date' and store the result in the 'coverage_date' column
sku_whse_oh_order['coverage_date'] = pd.to_datetime(sku_whse_oh_order['received_date']) + pd.to_timedelta(sku_whse_oh_order['coverage_day'], unit='D')

# Date formating, date calculating

# Replace missing values in 'promo_fl' column with 'N'
sku_whse_oh_order['promo_fl'].replace({'':'N'}, inplace=True)

# Convert the 'lt_days' column to float and then to integer type
sku_whse_oh_order['lt_days'] = sku_whse_oh_order['lt_days'].astype(float).astype(int)

# Convert the 'first_ship_date' column to datetime format
sku_whse_oh_order['first_ship_date'] = pd.to_datetime(sku_whse_oh_order['first_ship_date'])
sku_whse_oh_order['received_date'] = pd.to_datetime(sku_whse_oh_order['received_date'])

# Convert the 'transit_date' column to a YYYY-MM-DD string format and then back to datetime format
sku_whse_oh_order['transit_date'] = pd.to_datetime(sku_whse_oh_order['transit_date'])


# Calculate a new column 'calculated_date' based on 'transit_date' and 'first_ship_date' where if 'transit_date' is NaN, use 'first_ship_date'
sku_whse_oh_order["calculated_date"] = sku_whse_oh_order.apply(lambda row: row["transit_date"] if not pd.isna(row["transit_date"]) else row["first_ship_date"], axis=1)

# Add the 'lt_days' to the 'calculated_date' to get the adjusted date
sku_whse_oh_order["calculated_date"] = sku_whse_oh_order["calculated_date"] + pd.to_timedelta(sku_whse_oh_order['lt_days'], unit='D')

# Filter rows where 'calculated_date' + 'lt_days' is less than or equal to the 'coverage_date'
filter_sku_whse_oh_order = sku_whse_oh_order[sku_whse_oh_order.calculated_date <= sku_whse_oh_order.coverage_date]


whse_sku_orders = filter_sku_whse_oh_order.copy()

# Convert date columns to datetime objects

# Convert the numerical column to integer type
whse_sku_orders['ord_units'] = whse_sku_orders['ord_units'].astype(int)
whse_sku_orders['rcvd_units'] = whse_sku_orders['rcvd_units'].astype(int)


# Calculate A, B, C for each group and then calculated_short_term_oo
def calculate_new_ord_units(group):
    A = group.loc[(group['calculated_date'] <= group['received_date']),'ord_units'].sum() # calculated date(transit date or first_ship_date) less or equal than received_date
                  
    B = group.loc[group['calculated_date'] <= group['received_date'], 'rcvd_units'].sum()
    
    C = group.loc[(group['calculated_date'] > group['received_date']) ,'ord_units'].sum() ## our date is less than coverage date so we can calculate as short term oo,
                                                                                           #promote flag yes or N
    
    group['calculated_short_term_oo'] = A - B + C
    return group

# Group by 'sku' and 'input_source_store' and apply the 'calculate_new_ord_units' function on each group
whse_sku_orders = whse_sku_orders.groupby(['sku', 'input_source_store']).apply(calculate_new_ord_units)

# Calculate the 'calculated_short_term_oo' column by multiplying it with 'ppk_qty'
whse_sku_orders['calculated_short_term_oo'] = whse_sku_orders['calculated_short_term_oo'] * whse_sku_orders['ppk_qty']

# Calculate A, B for each group and calculated_short_term_oo first_period_short_term_oo
def calculate_new_ord_units1(group):
    A = group.loc[(group['calculated_date'] <= group["received_date"]), 'ord_units'].sum() 
    B = group.loc[group['calculated_date'] <= group["received_date"], 'rcvd_units'].sum() 
    
    group['first_period_short_term_oo'] = A - B 
    return group

# Group by 'sku' and 'input_source_store' and apply the 'calculate_new_ord_units1' function on each group
whse_sku_orders = whse_sku_orders.groupby(['sku', 'input_source_store']).apply(calculate_new_ord_units1)

# Calculate the 'first_period_short_term_oo' column by multiplying it with 'ppk_qty'
whse_sku_orders['first_period_short_term_oo'] = whse_sku_orders['first_period_short_term_oo'] * whse_sku_orders['ppk_qty']

# Calculate the 'second_period_short_term_oo' column by subtracting 'first_period_short_term_oo' from 'calculated_short_term_oo'
whse_sku_orders['second_period_short_term_oo'] = (whse_sku_orders['calculated_short_term_oo'] - whse_sku_orders['first_period_short_term_oo'])

# whse_sku_level (no first_ship_date etc)

whse_orders = whse_sku_orders.copy()
whse_orders = whse_sku_orders.groupby(['input_source_store', 'parent_sku', 'baby_sku', 'parent_oh', 'baby_oh','ppk_qty', 'lt_days','num_of_store','sku', 'style',
                                       'color','coverage_date','first_period_short_term_oo','second_period_short_term_oo']).first().reset_index()
whse_orders = whse_orders[['input_source_store', 'parent_sku', 'baby_sku', 'parent_oh', 'baby_oh','ppk_qty', 'lt_days','num_of_store','sku', 'style',
                          'color','coverage_date','first_period_short_term_oo','second_period_short_term_oo']]

#find the sku in sku_whse_oh_order   but not in whse_orders.
sku_notin_whse_orders = [parent_sku for parent_sku in sku_whse_oh_order.parent_sku.unique() if parent_sku not in whse_orders.parent_sku.unique()]

#convert to dataframe
sku_notin_whse_orders_df = sku_whse_oh_order[(sku_whse_oh_order['parent_sku'].isin(sku_notin_whse_orders))]

# fill the first_period_short_term_oo and second_period_short_term_oo with zero 
sku_notin_whse_orders_df = sku_notin_whse_orders_df[['input_source_store','parent_sku', 'baby_sku','sku', 'style','color','lt_days','num_of_store','parent_oh','baby_oh', 'ppk_qty','coverage_date']]
sku_notin_whse_orders_df[['first_period_short_term_oo','second_period_short_term_oo']] = 0

# concat the dataframes. All data now have all info
whse_orders = pd.concat([whse_orders,sku_notin_whse_orders_df],ignore_index=True)

# copy on_hand_df_db2 to get selling store on hand df
store_on_hand_df = on_hand_df_db2.copy()


#join store_on_hand_df and  store_sku_orders_df

store_onhand_orders = pd.merge(store_on_hand_df,store_sku_orders_df, 
                                           left_on=['parent_sku','store'],
                                           right_on=['sku','store'],
                                           how='left')

# Remove columns ending with '_y'
store_onhand_orders = store_onhand_orders.drop([col for col in store_onhand_orders if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
store_onhand_orders.columns = [col.replace('_x', '') for col in store_onhand_orders.columns]

#fill the Nan columns with correct values

store_onhand_orders['item_id'] = store_onhand_orders['parent_sku'].map(input_sku_df.set_index('sku')['item_id'])
store_onhand_orders['style'] = store_onhand_orders['parent_sku'].map(input_sku_df.set_index('sku')['style'])
store_onhand_orders['color'] = store_onhand_orders['parent_sku'].map(input_sku_df.set_index('sku')['color'])
store_onhand_orders['sku'] = store_onhand_orders['parent_sku']
store_onhand_orders

# to have style.color,provided_date,coverage date join with input_sku_df

sku_store_oh_order = pd.merge(store_onhand_orders,input_sku_df,
                                           left_on=['parent_sku'],
                                           right_on=['sku'],
                                           how='inner')
# Remove columns ending with '_y'
sku_store_oh_order = sku_store_oh_order.drop([col for col in sku_store_oh_order if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
sku_store_oh_order.columns = [col.replace('_x', '') for col in sku_store_oh_order.columns]

#Calculating of short_term_oo_from_vndr for store


# Convert the 'first_ship_date' column to a datetime format and then format it to 'YYYY-MM-DD' string format
sku_store_oh_order['first_ship_date'] = pd.to_datetime(sku_store_oh_order['first_ship_date']).dt.strftime('%Y-%m-%d')
sku_store_oh_order['transit_date'] = pd.to_datetime(sku_store_oh_order['transit_date']).dt.strftime('%Y-%m-%d')

# Compute the 'received_date' by adding the 'lt_days' (lead time days) to the 'provided_date' and store the result in the 'received_date' column
sku_store_oh_order['received_date'] = pd.to_datetime(sku_store_oh_order['provided_date']) + pd.to_timedelta(sku_store_oh_order['lt_days'], unit='D')

# Compute the 'coverage_date' by adding the 'coverage_day' to the 'received_date' and store the result in the 'coverage_date' column
sku_store_oh_order['coverage_date'] = pd.to_datetime(sku_store_oh_order['received_date']) + pd.to_timedelta(sku_store_oh_order['coverage_day'], unit='D') 

# Date formating, date calculating

# Replace missing values in 'promo_fl' column with 'N'
sku_store_oh_order['promo_fl'].replace({'':'N'}, inplace=True)

# Convert the 'lt_days' column to float and then to integer type
sku_store_oh_order['lt_days'] = sku_store_oh_order['lt_days'].astype(float).astype(int)

# Convert the 'first_ship_date','transit_date' column to datetime format
sku_store_oh_order['first_ship_date'] = pd.to_datetime(sku_store_oh_order['first_ship_date'])
sku_store_oh_order['transit_date'] = pd.to_datetime(sku_store_oh_order['transit_date'])

# Calculate a new column 'calculated_date' based on 'transit_date' and 'first_ship_date' where if 'transit_date' is NaN, use 'first_ship_date'
sku_store_oh_order["calculated_date"] = sku_store_oh_order.apply(lambda row: row["transit_date"] if not pd.isna(row["transit_date"]) else row["first_ship_date"], axis=1)

# Add the 'lt_days' to the 'calculated_date' to get the adjusted date
sku_store_oh_order["calculated_date"] = sku_store_oh_order["calculated_date"] + pd.to_timedelta(sku_store_oh_order['lt_days'], unit='D')

# Filter rows where 'first_ship_date' + 'lt_days' is less than or equal to the 'coverage_date'
filter_sku_store_oh_order = sku_store_oh_order[sku_store_oh_order.calculated_date + pd.to_timedelta(sku_store_oh_order['lt_days'], unit='D') <= sku_store_oh_order.coverage_date]

store_sku_orders = filter_sku_store_oh_order.copy()

# Convert the numerical column to integer type
store_sku_orders['ord_units'] = store_sku_orders['ord_units'].astype(int)
store_sku_orders['rcvd_units'] = store_sku_orders['rcvd_units'].astype(int)

# Current date
current_date = datetime.now()

# Calculate A, B, C for each group and then calculated_short_term_oo
def calculate_new_ord_units(group):
    A = group.loc[(group['calculated_date'] <= group['received_date']),'ord_units'].sum() # calculated date(transit date or first_ship_date) less or equal than received_date
                  
    B = group.loc[group['calculated_date'] <= group['received_date'], 'rcvd_units'].sum()
    
    C = group.loc[(group['calculated_date'] > group['received_date']) ,'ord_units'].sum()
    
    group['calculated_shortperiod_store_oo'] = A - B + C
    return group

# Group by 'sku' and 'input_source_store' and apply the 'calculate_new_ord_units' function on each group
store_sku_orders = store_sku_orders.groupby(['sku', 'store']).apply(calculate_new_ord_units)

# Calculate the 'calculated_short_term_oo' column by multiplying it with 'ppk_qty'
store_sku_orders['calculated_shortperiod_store_oo'] = store_sku_orders['calculated_shortperiod_store_oo'] * store_sku_orders['ppk_qty']

# Calculate A, B for each group and calculated_short_term_oo first_period_short_term_oo
def calculate_new_ord_units1(group):
    A = group.loc[(group['calculated_date'] <= group["received_date"]), 'ord_units'].sum() 
    B = group.loc[group['calculated_date'] <= group["received_date"], 'rcvd_units'].sum() 
    
    group['first_period_store_oo'] = A - B 
    return group

# Group by 'sku' and 'input_source_store' and apply the 'calculate_new_ord_units1' function on each group
store_sku_orders = store_sku_orders.groupby(['sku', 'store']).apply(calculate_new_ord_units1)

# Calculate the 'calculated_short_term_oo' column by multiplying it with 'ppk_qty'
store_sku_orders['first_period_store_oo'] = store_sku_orders['first_period_store_oo'] * store_sku_orders['ppk_qty']

# Calculate the 'second_period_short_term_oo' column by subtracting 'first_period_short_term_oo' from 'calculated_short_term_oo'
store_sku_orders['second_period_store_oo'] = (store_sku_orders['calculated_shortperiod_store_oo'] - store_sku_orders['first_period_store_oo'])

#store_sku_level
store_orders = store_sku_orders.groupby(['input_source_store','parent_sku','baby_sku','sku','style',
                                         'color','store','sks_ord_level','store_oh','ppk_qty','provided_date',
                                         'received_date','coverage_date','num_of_store'])['first_period_store_oo', 'second_period_store_oo'].first().reset_index()
                                         
#find the sku in sku_store_oh_order but not in store_orders

sku_notin_orders = [sku for sku in sku_store_oh_order.parent_sku.unique() if sku not in store_orders.parent_sku.unique()]

# Get the length of the sku_notin_orders list
n = len(sku_notin_orders)

if n > 0:
    # Create the DataFrame using the existing SKUs that are not in orders
    sku_notin_orders_df = sku_store_oh_order[(sku_store_oh_order['parent_sku'].isin(sku_notin_orders))]
   
    # Filter the necessary columns
    sku_notin_orders_df = sku_notin_orders_df[['input_source_store', 'parent_sku', 'baby_sku', 'sku', 'style', 'color',
                                               'store', 'sks_ord_level', 'store_oh', 'ppk_qty', 'provided_date',
                                               'received_date', 'coverage_date','num_of_store']]
   
    # Add the two new columns with 0 values
    sku_notin_orders_df[['first_period_store_oo','second_period_store_oo']] = 0
else:
    sku_notin_orders_df = None

# If 'sku_notin_orders_df' exists, concatenate it with 'store_orders';
# otherwise, simply use a copy of 'store_orders' for the result.
if sku_notin_orders_df is not None:
    store_orders= pd.concat([store_orders, sku_notin_orders_df], ignore_index=True)
else:
    store_orders= store_orders.copy()
    
    
# Getting forecast in sku-selling store level from database based on item_id and baby_sku
print("Collecting forecast value for sku")

max_coverage_period = sku_store_oh_order['coverage_date'].max()
max_coverage_period = max_coverage_period.strftime('%Y-%m-%d %H:%M:%S')

baby_sku_list = str(tuple(on_hand_df_db2['baby_sku'].unique()))

# Open and read the SQL file located at the specified path
f_name = "/home/scortusr1/scripts/altunty/NEW_PROJECT/sql_file/sku_forecast.sql"
with open(f_name, "r") as sql_fn:
    sql_file = sql_fn.read()

# Replace the placeholder "{baby_sku_list}" with the actual 'baby_sku_list' and remove double quotes from the SQL file content
sql_file = sql_file.replace("{baby_sku_list}", baby_sku_list)
sql_file = sql_file.replace("{max_coverage_period}", max_coverage_period)
sql_file = sql_file.replace('"', '')

# Set the temporary path where the parquet file will be written
tmp_path = "/home/scortusr1/scripts/altunty/NEW_PROJECT/path/forecastdata/"

# Create the directory if it doesn't exist
os.makedirs(tmp_path, exist_ok=True)

# Fetch data using the modified SQL query and write the results as a parquet file to the specified path
t.fetch_data(sql_file).write.parquet(tmp_path, mode="overwrite")

# Read the parquet file back into a pandas DataFrame
sku_forecast_df = pd.read_parquet(tmp_path)


# Merge the 'result_df' DataFrame with the 'sku_forecast_df' DataFrame based on 'baby_sku' and 'store' columns
main_df = pd.merge(
    store_orders,
    sku_forecast_df,
    how='left',
    left_on=[ 'baby_sku','store'],
    right_on=['sku', 'store'])

# Remove columns ending with '_y'
main_df = main_df.drop([col for col in main_df if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
main_df.columns = [col.replace('_x', '') for col in main_df.columns]

# 


# Filter the 'main_df' DataFrame based on date conditions (first_period)

main_df['cal_date'] = pd.to_datetime(main_df['cal_date']).dt.date
main_df['coverage_date'] = pd.to_datetime(main_df['coverage_date']).dt.date
# Get the current date
current_date = datetime.today().date()

# Filter the 'main_df' DataFrame based on date conditions
filter_store_fcst_first_df = main_df[
    (main_df['cal_date'] < main_df['received_date']) &
    (main_df['cal_date'] >= current_date)
    ]

#  getting total forecast value of sku in stores during first period

store_fcst_first_period = filter_store_fcst_first_df.groupby(['parent_sku', 'baby_sku', 'store'])['forecast'].sum().reset_index()
store_fcst_first_period.rename(columns = {'forecast' :'first_period_total_forecast'}, inplace=True)

# Filter the 'main_df' DataFrame based on date conditions
filter_store_fcst_second_df = main_df[
    (main_df['cal_date'] <= main_df['coverage_date']) & 
    (main_df['cal_date'] > main_df['received_date'])
    ]

#  getting total forecast value of sku in stores during second period

store_fcst_second_period = filter_store_fcst_second_df.groupby(['parent_sku', 'baby_sku', 'store'])['forecast'].sum().reset_index()
store_fcst_second_period.rename(columns = {'forecast' :'second_period_total_forecast'}, inplace=True)

 #To see total forecast of selling stores for each sku and other info in one df
main_and_store_fcst_first_df = pd.merge(
    main_df,
    store_fcst_first_period,
    left_on=['parent_sku', 'baby_sku', 'store'],
    right_on=['parent_sku', 'baby_sku', 'store'],
    how='inner'
)

# Remove columns ending with '_y'
main_and_store_fcst_first_df = main_and_store_fcst_first_df.drop([col for col in main_and_store_fcst_first_df if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
main_and_store_fcst_first_df.columns = [col.replace('_x', '') for col in main_and_store_fcst_first_df.columns]

main_and_store_fcst_df = pd.merge(
    main_and_store_fcst_first_df,
    store_fcst_second_period,
    left_on=['parent_sku', 'baby_sku', 'store'],
    right_on=['parent_sku', 'baby_sku', 'store'],
    how='inner'

)
# Remove columns ending with '_y'
main_and_store_fcst_first_df = main_and_store_fcst_first_df.drop([col for col in main_and_store_fcst_first_df if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
main_and_store_fcst_first_df.columns = [col.replace('_x', '') for col in main_and_store_fcst_first_df.columns]

main_and_store_fcst_df.drop(columns=['cal_date', 'forecast'], inplace=True)
main_and_store_fcst_df.drop_duplicates(inplace=True)
main_and_store_fcst_df.reset_index(drop=True, inplace=True)


store_fcst = main_and_store_fcst_df.copy()

# Calculate expected demand and add a new column 'exp_demand' to 'final_df'
store_fcst.loc[:, 'expected_demand'] = store_fcst['first_period_total_forecast'].round()
store_fcst['second_period_total_forecast'] = store_fcst['second_period_total_forecast'].round()


# Calculate replenishment order qty
store_fcst['Replenishment_order'] = np.where(store_fcst['store_oh'] - store_fcst['expected_demand'] < store_fcst['sks_ord_level'],
                                           'Y', 'N')
store_fcst['Replenishment_order_Qty'] = np.where(store_fcst['Replenishment_order'] == 'Y',
                                               store_fcst['sks_ord_level'] - (
                                                           store_fcst['store_oh'] + store_fcst['first_period_store_oo'] - store_fcst['expected_demand']),
                                               0)


store_fcst['Replenishment_order_Qty'] = np.where(store_fcst['Replenishment_order_Qty'] > 0,
                                                store_fcst['Replenishment_order_Qty'],0)

s =store_fcst.copy()

#Calculate total replenishment qty for warehouse

group1 = s.groupby(['parent_sku', 'baby_sku','input_source_store','store'])['Replenishment_order_Qty'].first().reset_index()
whs_Rplsh_Order = group1.groupby(['parent_sku', 'baby_sku', 'input_source_store'])['Replenishment_order_Qty'].sum().reset_index()
whs_Rplsh_Order.rename (columns = {'Replenishment_order_Qty':'Total_Replenishment_Order_Qty'}, inplace=True)


# Join dfs to show all data and Total_Replenishment_Order_Qty in one df

whse_df = pd.merge(whse_orders,whs_Rplsh_Order
                   ,left_on=['parent_sku','baby_sku','input_source_store']  
                   ,right_on=['parent_sku','baby_sku','input_source_store'] 
                   ,how='left') 
# Remove columns ending with '_y'
whse_df = whse_df.drop([col for col in whse_df if col.endswith('_y')], axis=1)

# Rename columns ending with '_x' by removing the suffix
whse_df.columns = [col.replace('_x', '') for col in whse_df.columns]

# fill with zero to Total_Replenishment_Order_Qty that has nan values
whse_df['Total_Replenishment_Order_Qty'] = whse_df['Total_Replenishment_Order_Qty'].fillna(0)

# Calculate Projected_warehouse_oh

prjctd_whse_oh = whse_df.copy()

prjctd_whse_oh["Projected_warehouse_oh"] = prjctd_whse_oh["baby_oh"]+ prjctd_whse_oh["first_period_short_term_oo"] - prjctd_whse_oh["Total_Replenishment_Order_Qty"]

        
prjctd_whse_oh['Projected_warehouse_oh'] = np.where(prjctd_whse_oh['Projected_warehouse_oh'] > 0,
                                                prjctd_whse_oh['Projected_warehouse_oh'],0)

#calculate total_prjctd_whse_oh and Total_second_period_short_term_oo

total_prjctd_whse_oh = prjctd_whse_oh.groupby(['parent_sku','baby_sku','ppk_qty', 'input_source_store','num_of_store','coverage_date'])['second_period_short_term_oo','Projected_warehouse_oh'].sum().reset_index()

total_prjctd_whse_oh.rename (columns = {'Projected_warehouse_oh':'Total_Projected_warehouse_oh'}, inplace=True)

total_prjctd_whse_oh.rename (columns = {'second_period_short_term_oo':'Total_second_period_short_term_oo'}, inplace=True)

# Calculate whse_total_fcst and Total_second_period_store_oo from main_and_store_fcst_d

whse_total_fcst = main_and_store_fcst_df.groupby(['parent_sku','baby_sku','input_source_store'])[['second_period_store_oo','second_period_total_forecast']].sum().reset_index()

#join total_prjctd_whse_oh and whse_total_fcst to calculate recomended_order_qty
whse_poh_fcst = pd.merge(total_prjctd_whse_oh , whse_total_fcst,left_on=['parent_sku','baby_sku','input_source_store']
            ,right_on=['parent_sku','baby_sku','input_source_store'],
             how ='left')

#Calculate recomended_order_qty

recomended_ord_qty = whse_poh_fcst.copy()

recomended_ord_qty ["recomended_order_qty"] = (recomended_ord_qty['second_period_total_forecast'] -  recomended_ord_qty['second_period_store_oo']) - (recomended_ord_qty['Total_second_period_short_term_oo'] +  recomended_ord_qty['Total_Projected_warehouse_oh']) 
       
recomended_ord_qty['recomended_order_qty'] = np.where(recomended_ord_qty['recomended_order_qty'] > 0,
                                                recomended_ord_qty['recomended_order_qty'],0)
recomended_ord_qty 

# Creating a dictionary to map parent SKUs to sets of associated baby SKUs
parent_sku_to_baby_sku = {}

# Iterating through the rows of the DataFrame
for index, row in recomended_ord_qty.iterrows():
    parent_sku = row["parent_sku"]
    baby_sku = row["baby_sku"]
    
    # If parent SKU is not already in the dictionary, add it with an empty set
    if parent_sku not in parent_sku_to_baby_sku:
        parent_sku_to_baby_sku[parent_sku] = set()
    
    # Adding the baby SKU to the set of associated baby SKUs
    parent_sku_to_baby_sku[parent_sku].add(baby_sku)

# Define a function 'calculate_levels' that calculates parent and baby SKU levels based on certain conditions
def calculate_levels(row):
    # Extract the parent SKU from the row
    parent_sku = row["parent_sku"]
    
    # Get the corresponding baby SKUs for the parent SKU from the 'parent_sku_to_baby_sku' dictionary
    baby_skus = parent_sku_to_baby_sku[parent_sku]
    
    # Check if there is only one baby SKU and if it matches the parent SKU or the row's baby SKU
    if len(baby_skus) == 1 and (list(baby_skus)[0] == parent_sku or list(baby_skus)[0] == row["baby_sku"]):
        # Calculate parent level based on the recommended order quantity and PPK quantity
        row["parent_level_pre"] = round(row["recomended_order_qty"] / row["ppk_qty"])
        row["recomended_order_units"] = row["parent_level_pre"]
    else:
        # Calculate parent level based on the maximum recommended order quantity and PPK quantity for the parent SKU
        rq_max = recomended_ord_qty[recomended_ord_qty["parent_sku"] == parent_sku]["recomended_order_qty"].max()
        ppk_max = recomended_ord_qty[recomended_ord_qty["parent_sku"] == parent_sku]["ppk_qty"].max()
        row["parent_level_pre"] = round(row["recomended_order_qty"] / ppk_max)
        row["recomended_order_units"] = round(rq_max / ppk_max)
    
    return row

# Apply the 'calculate_levels' function to each row in the DataFrame
recomended_ord_qty = recomended_ord_qty.apply(calculate_levels, axis=1)

recomended_ord_qty_df = recomended_ord_qty[['input_source_store','parent_sku','baby_sku','recomended_order_units','num_of_store','coverage_date']].copy()
recomended_ord_qty_df['request_id'] = request_id
recomended_ord_qty_df.rename(columns= {'coverage_date': 'next_ship_date'},inplace=True) 

missing_skus_df = pd.DataFrame({'sku' : missing_skus})
missing_skus_df['request_id']= request_id

t.sc.createDataFrame(recomended_ord_qty_df).repartition(1).write.parquet(f'{data_path}{request_id}/tmp', mode='overwrite')


t.push_data(f'{data_path}{request_id}/tmp', table_name, fastload=False) 

end_time = time.time()

print (f" The Order Projection process took {end_time - start_time}.seconds to get the results")

print(  f"""SELECT * FROM SCSOR_DB_LAB.order_projection_recomended_order_units WHERE REQUEST_ID ='{request_id}';""")

t.sc.createDataFrame(missing_skus_df).repartition(1).write.parquet(f'{data_path}{request_id}/tmp',mode='overwrite')

t.push_data(f'{data_path}{request_id}/tmp', missing_sku_table,fastload=False)

print( 'There are some skus dont have any active store.To see the skus:' )

print(f"""SELECT * FROM SCSOR_DB_LAB.order_projection_missing_skus WHERE REQUEST_ID ='{request_id}';""" )