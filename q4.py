import pandas
import sys
df = pandas.read_table('output', index_col=1, encoding='utf-8', header=None)
df.describe().to_csv(sys.stdout, encoding='utf-8',
                     float_format='%.2f', header=False)
