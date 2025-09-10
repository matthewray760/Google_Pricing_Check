import pandas as pd
import numpy as np
from aeml import run_aeml
from aemj import run_aemj

recon_date = '2025-09-08'

filepath = fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\input\test_file.csv'




df_aeml = run_aeml(194358,recon_date,filepath,sleeve_agg=198961)

df_aemj = run_aemj(194359,recon_date,filepath,sleeve_agg= 198960)


 #df.to_excel(fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\output\test_file.xlsx', index=False)
