import xmlrpc.client
import pandas as pd
import numpy as np
from dotenv import dotenv_values
import ast
import re
import math

df = pd.read_excel("files/relations/ar/aged_receivable-08-31-2022-700am.xlsx")
print(df)
print(df["Unnamed: 0"])
print(df.columns.values)
columns_ar = df.columns.values
columns_ar = np.append(columns_ar,"Customer Name")
print(columns_ar)
df_final = pd.DataFrame(columns=columns_ar)
customer_name = ""

for index, row in df.iterrows():
    
    if(pd.isna(row["Report Date"])):
        customer_name = row['Unnamed: 0']
    else:
        # print(type(row))
        # print(customer_name)
        row["Customer Name"] = customer_name
        df_final.loc[len(df_final)] =  row
        # print(row)

print(df_final)
df_final.to_csv("files/to_bc_outputs/ar_test.csv")


    # print(row['Unnamed: 0'])

# for ind in df.index:
#     print(df.loc[ind, "Report Date"])
#     print(ind)