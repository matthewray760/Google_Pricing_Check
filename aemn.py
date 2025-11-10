import pandas as pd
import numpy as np
from sql import sql_alternate_id
from sql import sql_sec_mappings
from sql import sql_cusip_id
from sql import sql_prom_pos

def run_aemn(account_id,recon_date,filepath,sleeve_agg):
    filepath = filepath
    account_id = account_id
    recon_date = recon_date

    df = pd.read_csv(filepath)
    aemn_df = df[df['Account ID']== account_id]
   


    ### IRS
    aemn_irs = aemn_df[aemn_df['Security Type'] == 'IRS']
    aemn_irs = aemn_irs.drop_duplicates(subset='Identifier', keep='first')
    aemn_irs = aemn_irs[['Account ID','Identifier','Security ID']]
    aemn_irs['Identifier'] = aemn_irs['Identifier'].str.replace(r'^.', 'B', regex=True)
    identifier_list = "', '".join(aemn_irs['Identifier'].tolist())
    identifier_list = f"'{identifier_list}'"
    

    sql_df = sql_alternate_id(account_id,identifier_list,recon_date)[0]
    sql_df.rename(columns={'AlternateId': 'Identifier'}, inplace=True)

   
    merged_df = pd.merge(aemn_irs,sql_df, on = 'Identifier')
    merged_df.rename(columns = {'Identifier': 'AlternateId', 'Account ID': 'PartitionId','Security ID': 'SecurityId'}, inplace=True)
    merged_df['PartitionType'] = '0'

    final_df_irs = merged_df[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]


    ### Future
    aemn_future = aemn_df[aemn_df['Security Type'] == 'FUTURE']
    aemn_future = aemn_future.drop_duplicates(subset='Identifier', keep='first')
    aemn_future = aemn_future[['Account ID','Identifier','Security ID']]
    identifier_list = "', '".join(aemn_future['Identifier'].tolist())
    identifier_list = f"'{identifier_list}'"

    sql_mapping_df = sql_sec_mappings(sleeve_agg,identifier_list)[0]
    custody_identifiers = "', '".join(sql_mapping_df['Custody Cusip'].tolist())
    custody_identifiers = f"'{custody_identifiers}'"

    sql_df_f = sql_cusip_id(account_id,identifiers=custody_identifiers,recon_date=recon_date)[0]
    sql_mapping_df.rename(columns= {'Custody Cusip': 'Cusip'}, inplace=True)
 

    merged_df_f = pd.merge(sql_mapping_df,sql_df_f, on = 'Cusip')
    
    merged_df_f.rename(columns = {'AlternateId_x': 'AlternateId','SecurityID': 'SecurityId','MaturityDate_x':'MaturityDate','Ticker_x':'Ticker'}, inplace=True)

    
    merged_df_f['PartitionType'] = '0'
    merged_df_f['PartitionId'] = account_id

    final_df_f = merged_df_f[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]






    ### Option
    aemn_option = aemn_df[aemn_df['Security Type'] == 'OPTION']
    aemn_option = aemn_option.drop_duplicates(subset='Identifier', keep='first')
    aemn_option = aemn_option[['Account ID','Identifier','Security ID']]
    
    identifier_list = aemn_option['Identifier'].astype(str).tolist()
    
    ad_list = [identifier for identifier in identifier_list if identifier.startswith('AD')]
    non_ad_list = [identifier for identifier in identifier_list if not identifier.startswith('AD')]
    non_ad_list = "', '".join (non_ad_list)
    non_ad_list = f"'{non_ad_list}'"
    
    print("non-ad list is", non_ad_list)

    
    sql_ad_df = pd.DataFrame()

    
    if len(ad_list)>0:
        sql_ad_df = sql_cusip_id(account_id,ad_list)
        sql_ad_df.rename(columns= {'Custody Cusip': 'Cusip'}, inplace=True)
        
    else:
        pass

    sql_nad_df = pd.DataFrame()
    
    if len(non_ad_list) > 0:
        sql_mapping_df = sql_sec_mappings(sleeve_agg,non_ad_list)[0]
        
        
        custody_identifiers = "', '".join(sql_mapping_df['Custody Cusip'].tolist())
        custody_identifiers = f"'{custody_identifiers}'"
        sql_cusip_mapping = sql_cusip_id(account_id,identifiers=custody_identifiers,recon_date=recon_date)[0]
        sql_nad_df = pd.merge(sql_cusip_mapping,sql_mapping_df, left_on='Cusip', right_on='Custody Cusip', how = 'inner')

        #sql_nad_df.to_excel(fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\output\sql_nad_df.xlsx', index=False)
    else:
        pass


    if sql_ad_df.empty:
        merged_df_total = sql_nad_df  # Use the non-empty DataFrame if the other is empty
    elif sql_nad_df.empty:
        merged_df_total = sql_ad_df  # Use the non-empty DataFrame if the other is empty
    else:
        merged_df_total = pd.merge(sql_ad_df, sql_nad_df, on='Cusip', how='inner')
 


    
    merged_df_total.rename(columns = {'AlternateId_x': 'AlternateId','SecurityID': 'SecurityId','MaturityDate_x':'MaturityDate','Ticker_x':'Ticker'}, inplace=True)

    
    merged_df_total['PartitionType'] = '0'
    merged_df_total['PartitionId'] = account_id

    final_df_o = merged_df_total[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]

    
    



    return final_df_irs, final_df_f, final_df_o
