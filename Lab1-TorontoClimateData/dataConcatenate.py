#!/usr/bin/python3

import sys
import pandas as pd
import glob
import os

input_path, output_path = sys.argv[1], sys.argv[2]
filename = 'all_years.csv'
cvsFiles = glob.glob(os.path.join(input_path , "*.csv"))


li = []
for f in cvsFiles:
    df = pd.read_csv(f, index_col=None, header=0)
    li.append(df)

df_concat = pd.concat(li,axis=0, ignore_index=True)
df_concat.to_csv(f"{output_path}/{filename}")