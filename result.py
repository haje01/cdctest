
import pdb
import time

with open(snakemake.input[0], 'rt') as f:
    st = int(f.read())

total = 0
with open(snakemake.output[0], 'wt') as fw:
    pcnt = 0
    for fn in snakemake.input[1:]:
        pcnt += 1
        with open(fn, 'rt') as fr:
            lines = fr.read().split('\n')
            result = lines[-2] + '\n'
            cnt = int(result.split()[1])
            total += cnt
            fw.write(result)
    fw.write("======================\n")
    elapsed = time.time() - st
    vel = int(total / elapsed)
    fw.write(f'Elapsed time: {elapsed:.2f}\n');
    msg = f"Total Performance: {vel} rows per seconds with {pcnt} agents.\n"
    fw.write(msg)
