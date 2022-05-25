
import pdb
import time

with open(snakemake.input[0], 'rt') as f:
    st = int(f.read())

tot_sel = tot_ins = 0
with open(snakemake.output[0], 'wt') as fw:
    proc_ins = proc_sel = 0
    # insert, select 결과 파일들 순회
    for fn in snakemake.input[1:]:
        with open(fn, 'rt') as fr:
            # 프로세스별 결과 파일의 마지막 줄
            lines = fr.read().split('\n')
            result = lines[-2] + '\n'
            # 파싱
            elms = result.split()
            cnt = int(elms[1])
            print(fn, elms[0])
            if elms[0] == "Insert":
                proc_ins += 1
                tot_ins += cnt
                fw.write(result)
            elif elms[0] == "Select":
                proc_sel += 1
                tot_sel += cnt
                fw.write(result)
    fw.write("======================\n")
    elapsed = time.time() - st
    vel_ins = int(tot_ins / elapsed)
    vel_sel = int(tot_sel / elapsed)
    fw.write(f'Elapsed time: {elapsed:.2f}\n');
    msg = f"Insert Performance: {vel_ins} rows per seconds with {proc_ins} agents.\n"
    fw.write(msg)
    msg = f"Select Performance: {vel_sel} rows per seconds with {proc_sel} agents.\n"
    fw.write(msg)
