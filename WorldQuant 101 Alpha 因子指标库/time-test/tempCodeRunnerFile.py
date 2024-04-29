import time
import pandas as pd
from tqdm import tqdm
from alpha101_adjusted import Alphas

data = pd.read_csv('../db/dataPerformance.csv')