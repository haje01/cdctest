import re

import pandas as pd

PTRN = re.compile(r'(\d+) rows per seconds')


def get_insel_rps(f):
    lines = f.readlines()
    ins = []
    sels = []
    for line in lines:
        match = PTRN.search(line)
        if match is None:
            continue
        cnt = int(match.groups()[0])
        if 'Insert' in line:
            ins.append(cnt)
        elif 'Selector' in line:
            sels.append(cnt)
    return ins, sels


def type_df(iidx, dtype):
    with open(snakemake.input[iidx], 'rt') as f:
        ins, sels = get_insel_rps(f)
        df = pd.DataFrame({'ins': ins, 'sels': sels})
        df = df.assign(type=dtype)
        return df

dfs = []
# DB 테스트 결과
dfs.append(type_df(0, 'db'))
# CT 테스트 결과
dfs.append(type_df(1, 'ct'))
# CDC 테스트 결과
dfs.append(type_df(2, 'cdc'))
df = pd.concat(dfs)
df.to_parquet(snakemake.output[0])