import dolphindb as ddb
import numpy as np

# import pandas as pd

data = [
    ['600007', '600104'],
    [np.datetime64('2019-01-01T20:01:01'), np.datetime64('2019-01-01T20:01:02')],
    [100.36, 99.3],
    [100.36, 101.22],
    [100.35, 100.45],
    [4138, 2],
    [20, 39],
    [1, 5],
]

s = ddb.session()
s.connect('127.0.0.1', 8848, 'admin', '123456')
s.run('tableInsert{level2}', data)
