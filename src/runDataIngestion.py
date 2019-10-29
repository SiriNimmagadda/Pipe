import sys
import os
import pandas as pd


if len(sys.argv) == 2:
    infile = sys.argv[1]
    print('Arguments', sys.argv[1])
else:
    print("Data Processing Pipeline Usage:runDataIngestion.py <Directory Path>")
    sys.exit(1)

DataFileList = os.listdir(infile)

data_dfs = pd.DataFrame()
for filename in DataFileList:
    df = pd.read_csv(os.path.join(infile, filename), index_col=False, header=0)
    data_dfs = data_dfs.append(df,ignore_index=True)

data_dfs.to_json(os.path.join(infile, 'final.json'),orient='records')