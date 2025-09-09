import pandas as pd
import numpy as np
from aeml import run_aeml

recon_date = '2025-09-08'

filepath = fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\input\test_file.csv'




df = run_aeml(194358,recon_date,filepath,sleeve_agg=198961)

print(df)


 #df.to_excel(fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\output\test_file.xlsx', index=False)
