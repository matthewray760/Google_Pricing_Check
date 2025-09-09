import pandas as pd
import numpy as np
from sql import sql_alternate_id
from sql import sql_sec_mappings
from sql import sql_cusip_id

def run_aeml(account_id,recon_date,filepath,sleeve_agg):
    filepath = filepath
    account_id = account_id
    recon_date = recon_date

    df = pd.read_csv(filepath)
    aeml_df = df[df['Account ID']== 194358]


    ### IRS
    aeml_irs = aeml_df[aeml_df['Security Type'] == 'IRS']
    aeml_irs = aeml_irs.drop_duplicates(subset='Identifier', keep='first')
    aeml_irs = aeml_irs[['Account ID','Identifier','Security ID']]
    aeml_irs['Identifier'] = aeml_irs['Identifier'].str.replace(r'^.', 'B', regex=True)
    identifier_list = "', '".join(aeml_irs['Identifier'].tolist())
    identifier_list = f"'{identifier_list}'"

    sql_df = sql_alternate_id(account_id,identifier_list,recon_date)[0]
    sql_df.rename(columns={'AlternateId': 'Identifier'}, inplace=True)


    merged_df = pd.merge(aeml_irs,sql_df, on = 'Identifier')
    merged_df.rename(columns = {'Identifier': 'AlternateId', 'Account ID': 'PartitionId','Security ID': 'SecurityId'}, inplace=True)
    merged_df['PartitionType'] = '0'

    final_df = merged_df[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]

    ### Future
    aeml_future = aeml_df[aeml_df['Security Type'] == 'FUTURE']
    aeml_future = aeml_future.drop_duplicates(subset='Identifier', keep='first')
    aeml_future = aeml_future[['Account ID','Identifier','Security ID']]
    identifier_list = "', '".join(aeml_future['Identifier'].tolist())
    identifier_list = f"'{identifier_list}'"

    sql_mapping_df = sql_sec_mappings(sleeve_agg,identifier_list)[0]
    custody_identifiers = "', '".join(sql_mapping_df['Custody Cusip'].tolist())
    custody_identifiers = f"'{custody_identifiers}'"

    sql_df_f = sql_cusip_id(account_id,identifiers=custody_identifiers,recon_date=recon_date)[0]
    print(sql_df_f.columns)
    print(sql_mapping_df.columns)
 

    merged_df_f = pd.merge(sql_mapping_df,sql_df_f, on = 'Cusip')
    print(merged_df_f.columns)
    merged_df_f.rename(columns = {'Identifier': 'AlternateId', 'Account ID': 'PartitionId','Security ID': 'SecurityId'}, inplace=True)
    merged_df_f['PartitionType'] = '0'

    final_df_f = merged_df_f[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]



    return final_df, final_df_f
