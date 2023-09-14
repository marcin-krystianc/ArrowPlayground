import pandas as pd
import matplotlib.pyplot as plt
import sys

plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True

path = sys.argv[1]
df = pd.read_csv(path, skipinitialspace=True)
print (df)

groups =  df.groupby(['chunk_size', 'rows'])
fig, ax = plt.subplots(len(groups), sharex=True)
i = 0
for ((chunk_size, rows), g) in groups:
    title = "chunk_size={}, rows={}".format(chunk_size, rows)
    print(title)
    g['writing(μs)'] = g['writing(μs)'] / g['columns']
    g['reading_all(μs)'] = g['reading_all(μs)'] / g['columns']
    g['reading_100(μs)'] = g['reading_100(μs)'] / 100

    ax[i].set_title(title)
    # ax[i].plot(g['columns'], g['writing(μs)'], label='writing(μs) (per column)', marker='o',)
    ax[i].plot(g['columns'], g['reading_all(μs)'], label='reading_all(μs) (per column)', marker='o',)
    ax[i].plot(g['columns'], g['reading_100(μs)'], label='reading_100(μs) (per column)', marker='o',)

    i+=1

fig.suptitle(path)
plt.legend()
plt.show()