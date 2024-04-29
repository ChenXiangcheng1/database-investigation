import h5py
import numpy as np


codes = ["000001.XSHE", "600000.XSHG"]
h5file = "tmp/bars.h5"
h5 = h5py.File(h5file, "a")

for name in ("1m", "5m", "10m", "30m", "1d"):
    if name not in h5.keys():
        h5.create_group(f"/{name}")  # 按行情周期频率创建group


x = np.arange(100)

with h5py.File('tmp/test.h5', 'w') as f:  # 创建文件
    f.create_dataset('test_numpy', data=x)  # dataset
    subgroup = f.create_group('subgroup')  # group
    subgroup.create_dataset('test_numpy', data = x)

f = h5py.File("test.h5", "r")
print(list(f.keys()))


# 感觉维护起来很麻烦 Python+HDF5