import pandas
df = pandas.read_table('output', index_col=1, encoding='utf-8', header=None)
print("%.2f" % df.iloc[:, 3].quantile(q=0.85))
