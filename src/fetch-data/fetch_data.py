"""
- Fetches data and converts to pandas data frame.
"""

import sys

sys.path.append("../utils")
from imports import *
from ucimlrepo import fetch_ucirepo

attr_start_count = 2
attr_end_count = 9

# fetch dataset from ucimlrepo
# data_uci = fetch_ucirepo(id=31) #power

# data (as pandas dataframes)
# X = pd.DataFrame(data_uci.data.features)
# Y = pd.DataFrame(data_uci.data.targets)
# df = X.iloc[:, :]
X = pd.read_csv("../utils/higgs/HIGGS.csv")
df = X.iloc[:, -7:]

df.columns = ["a", "b", "c", "d", "e", "f", "g"]
# df = X.iloc[:, -7:]
print(df.head())
df.to_csv("../utils/higgs/higgs_df.csv", index=False)
