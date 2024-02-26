import pandas as pd
from ucimlrepo import fetch_ucirepo

# fetch dataset from ucimlrepo
covertype = fetch_ucirepo(id=31)
# data (as pandas dataframes)
X = pd.DataFrame(covertype.data.features)
Y = pd.DataFrame(covertype.data.targets)
df = X.iloc[:, :10]  # final needed df
