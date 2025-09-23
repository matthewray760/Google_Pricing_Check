import pandas as pd
import numpy as np
from aeml import run_aeml
from aemj import run_aemj
from aemn import run_aemn

recon_date = '2025-09-22'

filename = 'price_check_9.22'

filepath = fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\input\{filename}.csv'

#test_file_option_8.31


aeml_irs, aeml_future, aeml_option = run_aeml(194358,recon_date,filepath,sleeve_agg=198961)


aemj_irs, aemj_future = run_aemj(194359,recon_date,filepath,sleeve_agg= 198960)

aemn_irs, aemn_future, aemn_option = run_aemn(216465,recon_date,filepath,sleeve_agg= 216470)


main_df = pd.concat([aeml_irs, aeml_future, aeml_option,aemj_irs, aemj_future,aemn_irs, aemn_future, aemn_option], ignore_index=True)

main_df.to_excel(fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python\Google_Pricing_Check\output\test_file.xlsx', index=False)
