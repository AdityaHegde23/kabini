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
data_uci = fetch_ucirepo(id=31)

# data (as pandas dataframes)
X = pd.DataFrame(data_uci.data.features)
Y = pd.DataFrame(data_uci.data.targets)
df = X.iloc[:, attr_start_count:attr_end_count]
