import pandas as pd
import matplotlib.pyplot as plt
import sys

plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True

path = sys.argv[1]
df = pd.read_csv(path, skipinitialspace=True)
print (df)

df['reading(μs)'] =    df['reading(μs)'] /    df['columns_to_read']
df['reading_p1(μs)'] = df['reading_p1(μs)'] / df['columns_to_read']
df['reading_p2(μs)'] = df['reading_p2(μs)'] / df['columns_to_read']

groups =  df.groupby(['chunk_size', 'rows', 'columns_to_read'])
fig, ax = plt.subplots(len(groups), 3, sharex=True)
i = 0
x_axis = 'columns'

for ((chunk_size, rows, columns_to_read), g) in groups:
    title = "chunk_size={}, rows={}, columns_to_read={}, TOTAL TIME".format(chunk_size, rows, columns_to_read)
    print(title)

    ax[i, 0].set_title(title)
    ax[i, 1].set_title("Opening File")
    ax[i, 2].set_title("Data Reading")

    ax[i, 0].set_ylabel("μs/column")
    ax[i, 1].set_ylabel("μs/column")
    ax[i, 2].set_ylabel("μs/column")

    for (name, g2) in g.groupby(['name']):        

        ax[i, 0].plot(g2[x_axis], g2['reading(μs)'], label=name, marker='.',)
        ax[i, 1].plot(g2[x_axis], g2['reading_p1(μs)'], label=name, marker='.',)
        ax[i, 2].plot(g2[x_axis], g2['reading_p2(μs)'], label=name, marker='.',)

        ax[i, 0].set_ylim(bottom=0, auto=True)
        ax[i, 1].set_ylim(bottom=0, auto=True)
        ax[i, 2].set_ylim(bottom=0, auto=True)
        ax[i, 0].grid(visible=True)
        ax[i, 1].grid(visible=True)
        ax[i, 2].grid(visible=True)
        ax[i, 2].legend()
        
    i+=1

ax[-1, 0].set_xlabel(x_axis)
ax[-1, 1].set_xlabel(x_axis)
ax[-1, 2].set_xlabel(x_axis)

fig.suptitle(path)
plt.show()