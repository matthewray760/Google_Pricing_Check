import pandas as pd
import numpy as np
from sql import sql_alternate_id
from sql import sql_sec_mappings
from sql import sql_cusip_id
from sql import sql_prom_pos

def run_aemx(account_id,recon_date,filepath,sleeve_agg):
    filepath = filepath
    account_id = account_id
    recon_date = recon_date

    df = pd.read_csv(filepath)
    aemx_df = df[df['Account ID']== 274044]


    ### IRS
    aemx_irs = aemx_df[aemx_df['Security Type'] == 'IRS']
    aemx_irs = aemx_irs.drop_duplicates(subset='Identifier', keep='first')
    aemx_irs = aemx_irs[['Account ID','Identifier','Security ID']]
    aemx_irs['Identifier'] = aemx_irs['Identifier'].str.replace(r'^.', 'S', regex=True)
    identifier_list = "', '".join(aemx_irs['Identifier'].tolist())
    identifier_list = f"'{identifier_list}'"

    sql_df = sql_alternate_id(account_id,identifier_list,recon_date)[0]
    sql_df.rename(columns={'AlternateId': 'Identifier'}, inplace=True)


    merged_df = pd.merge(aemx_irs,sql_df, on = 'Identifier')
    merged_df.rename(columns = {'Identifier': 'AlternateId', 'Account ID': 'PartitionId','Security ID': 'SecurityId'}, inplace=True)
    merged_df['PartitionType'] = '0'

    final_df_irs = merged_df[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]


    ### Future
    aemx_future = aemx_df[aemx_df['Security Type'] == 'FUTURE']
    aemx_future = aemx_future.drop_duplicates(subset='Identifier', keep='first')
    aemx_future = aemx_future[['Account ID','Identifier','Security ID']]
    identifier_list = "', '".join(aemx_future['Identifier'].tolist())
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
    aemx_option = aemx_df[aemx_df['Security Type'] == 'OPTION']
    aemx_option = aemx_option.drop_duplicates(subset='Identifier', keep='first')
    aemx_option = aemx_option[['Account ID','Identifier','Security ID']]
    
    identifier_list = aemx_option['Identifier'].astype(str).tolist()
    
    ad_list = [identifier for identifier in identifier_list if identifier.startswith('AD')]
    non_ad_list = [identifier for identifier in identifier_list if not identifier.startswith('AD')]
    
    ad_list = "', '".join (ad_list)
    ad_list = f"'{ad_list}'"
    non_ad_list = "', '".join (non_ad_list)
    non_ad_list = f"'{non_ad_list}'"
    
    print("ad list is", ad_list)

    
    sql_ad_df = pd.DataFrame()

    
    if len(ad_list)>0:
        sql_mapping_df = sql_prom_pos(ad_list,recon_date=recon_date,sleeve_agg=sleeve_agg)[0]

        custody_identifiers = "', '".join(sql_mapping_df['Custody Cusip'].tolist())
        custody_identifiers = f"'{custody_identifiers}'"
        sql_cusip_mapping = sql_cusip_id(account_id,identifiers=custody_identifiers,recon_date=recon_date)[0]
        sql_ad_df = pd.merge(sql_cusip_mapping,sql_mapping_df, left_on='Cusip', right_on='Custody Cusip', how = 'inner')

        
    else:
        pass

    sql_nad_df = pd.DataFrame()
    
    if len(non_ad_list) > 0:
        sql_mapping_df = sql_prom_pos(non_ad_list,recon_date=recon_date,sleeve_agg=sleeve_agg)[0]
        print(sql_mapping_df)
        
        
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
   
    merged_df_total.rename(columns = {'AlternateId_x': 'AlternateId','Security ID': 'SecurityId','MaturityDate_x':'MaturityDate','Ticker_x':'Ticker'}, inplace=True)

    
    merged_df_total['PartitionType'] = '0'
    merged_df_total['PartitionId'] = account_id

    final_df_o = merged_df_total[['PartitionId','PartitionType','SecurityId','AlternateId','Cusip','Isin','Sedol','Ticker','MaturityDate']]




    return final_df_irs, final_df_f, final_df_o
