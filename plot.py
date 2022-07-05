import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

sns.set_theme()

dfs = []
for inp in snakemake.input:
    ep = int(inp.split('/')[3])
    df = pd.read_parquet(inp)
    df = df.assign(epoch=ep)
    dfs.append(df)

df = pd.concat(dfs)
df = df.reset_index(drop=True)
df = df.astype({'epoch': str})

fig, axes = plt.subplots(1, 2, figsize=(15, 5))
axes[0].set(ylim=(0, 400))
axes[1].set(ylim=(80, 1200))
sns.lineplot(ax=axes[0], data=df, x='epoch', y='ins', hue='index').set(title="Insert Per Seconds (x 10)")
sns.lineplot(ax=axes[1], data=df, x='epoch', y='sels', hue='index').set(title="Select Per Seconds (x 4)")
fig.savefig(snakemake.output[0])