import pandas as pd
from dotenv import dotenv_values
import ast
import re
import math


df = pd.read_csv("files/relations/bc_ids.csv")
print(df)

items_df = pd.read_csv("../go_live/files/csvs/from_api/items.csv")

print(items_df)

def getMappingList(old_value,relation_csv,default_value):

    new_value = default_value
    if (old_value != False):
        find_new = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['new_value'].values[0]
    
    return(new_value)