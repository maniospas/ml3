# ml3

*A dynamic dependency manager for Python.*

Similarly to how compilers use target triplets that determine
platform details (machine,vendor,operating system), 
machine learning and other data pipelines in Python create 
unique combinations of components that determine which combinations
of dependencies work in practice.

As dependencies keep evolving, this set of combinations is bound
to change. This is where *ML3* comes and aims to dynamically
discover the appropriate combination of dependencies.
Its main use case is determining the dependencies for a target
triplet consisting of a) a data processing scheme, 
b) model computations, and c) some analysis or post-processing.

More or fewer components can be combined too.

## :zap: Quickstart

```python
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
    print("Input\n", df["x"])
    return float(np.mean(df["x"]))

result = assess(load_data)   # Creates .ml3/assess-load_data.txt
print("Result:", result)
```