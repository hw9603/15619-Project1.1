import pandas
monthly_view = pandas.read_table('output', index_col=1, encoding='utf-8',
                                 header=None).iloc[:, 0]
join_result = pandas.read_table('teams.txt', index_col=0, encoding='utf-8') \
            .join(monthly_view, how='inner')
print(join_result.rank(method='dense', ascending=False, axis='index') \
        .iloc[:, 0].to_json(double_precision=0))
