import pandas
df = pandas.read_table('output', index_col=1, encoding='utf-8', header=None)
print(df.iloc[:, 0].head().to_json())
