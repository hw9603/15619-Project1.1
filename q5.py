import pandas
df = pandas.read_table('output', index_col=1, encoding='utf-8', <params>)
print("%.2f" % df.iloc[<positions>].<method>)