import pandas as pd
import numpy as np
from sql import sql_alternate_id
from sql import sql_sec_mappings
from sql import sql_cusip_id

def run_aemj(account_id,recon_date,filepath,sleeve_agg):
    filepath = filepath
    account_id = account_id
    recon_date = recon_date

    df = pd.read_csv(filepath)
    aemj_df = df[df['Account ID']== 194359]


    ### IRS
    aemj_irs = aemj_df[aemj_df['Security Type'] == 'IRS']
    aemj_irs = aemj_irs.drop_duplicates(subset='Identifier', keep='first')
    aemj_irs = aemj_irs[['Account ID','Identifier','Security ID']]
    aemj_irs['Identifier'] = aemj_irs['Identifier'].str.replace(r'^.', 'B', regex=True)
    identifier_list = "', '".join(aemj_irs['Identifier'].tolist())
    identifier_list = f"'{identifier_list}'"

    sql_df = sql_alternate_id(account_id,identifier_list,recon_date)[0]
    sql_df.rename(columns={'AlternateId': 'Identifier'}, inplace=True)


    merged_df = pd.merge(aemj_irs,sql_df, on = 'Identifier')
    merged_df.rename(columns = {'Identifier': 'AlternateId', 'Account ID': 'PartitionId','Security ID': 'SecurityId'}, inplace=True)
    merged_df['PartitionType'] = '0'

    final_df = merged_df[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]


    ### Future
    aemj_future = aemj_df[aemj_df['Security Type'] == 'FUTURE']
    aemj_future = aemj_future.drop_duplicates(subset='Identifier', keep='first')
    aemj_future = aemj_future[['Account ID','Identifier','Security ID']]
    identifier_list = "', '".join(aemj_future['Identifier'].tolist())
    identifier_list = f"'{identifier_list}'"

    sql_mapping_df = sql_sec_mappings(sleeve_agg,identifier_list)[0]
    custody_identifiers = "', '".join(sql_mapping_df['Custody Cusip'].tolist())
    custody_identifiers = f"'{custody_identifiers}'"

    sql_df_f = sql_cusip_id(account_id,identifiers=custody_identifiers,recon_date=recon_date)[0]
    sql_mapping_df.rename(columns= {'Custody Cusip': 'Cusip'}, inplace=True)
 

    merged_df_f = pd.merge(sql_mapping_df,sql_df_f, on = 'Cusip')
    
    merged_df_f.rename(columns = {'AlternateId_x': 'AlternateId','SecurityID': 'SecurityId','MaturityDate_x':'MaturityDate','Ticker_x':'Ticker'}, inplace=True)
    print(merged_df_f.columns)
    
    merged_df_f['PartitionType'] = '0'
    merged_df_f['PartitionId'] = account_id

    final_df_f = merged_df_f[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]
    print(final_df_f.head())
    final_df_f.to_excel(fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\output\test_file.xlsx', index=False)



    return final_df, final_df_f
