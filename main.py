from ml3.logger import Logger
from ml3.env import env

logger = Logger()

@env(packages=("pandas",), logger=logger)
def load_data():
    import numpy as np
    import pandas as pd
    return pd.DataFrame({"x": np.arange(5)*2})

@env(logger=logger)
def assess(loader):   # default argument is env-decorated
    import numpy as np
    df = loader()
    print(df["x"])
    return float(np.mean(df["x"]))

mean = assess(load_data)   # Creates .ml3/assess-load_data.txt
print("mean", mean)
