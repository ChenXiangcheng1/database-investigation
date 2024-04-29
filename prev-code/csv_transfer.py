import csv
import sys

def load_csv(path: str, out):    
    with open(path) as csv_file:
        reader = csv.reader(csv_file)
        keys = reader.__next__()

        for row in reader:
            convert_csv(row)
            out.write(','.join(row))
            out.write('\n')


def convert_csv(row: list):
    '''
    将ddb的csv文件转换为ch的csv文件
    date: yyyy.mm.dd -> yyyy-mm-dd
    '''
    
    date_idx = 1
    time_idx = 2
    sep = '-'
    row[date_idx] = row[date_idx][:4] + sep + row[date_idx][5:7] + sep + row[date_idx][8:]
    row[time_idx] = row[time_idx][:4] + sep + row[time_idx][5:7] + sep + row[time_idx][8:10] + " " + row[time_idx][11:19]
    
    # row[time_idx] = row[time_idx] if len(row[time_idx]) == 8 else ('0' + row[time_idx])
    # row[time_idx] = '"' + row[date_idx] + ' ' + row[time_idx] + '"'


if __name__ == "__main__":
    for path in sys.argv[1:]:
        load_csv(path, sys.stdout)
    # load_csv("D:/qq109/Desktop/trades_big.csv", sys.stdout)
    # load_csv("/d/qq109/Desktop/trades_big.csv", sys.stdout)
