import pandas as pd
from insolver.feature_monitoring import HomogeneityReport

import pyodbc
import configparser


config = configparser.ConfigParser()
config.read('config')
config_dict = {}
features = config.sections()

for feat in features:
    properties = dict(config[feat])
    config_dict[feat] = properties


#### Fixable part
#################################################
conn = pyodbc.connect("""DRIVER={PostgreSQL};
                         SERVER=localhost;
                         DATABASE=irek;
                         UID=irek;
                         PWD=0r7o9mz58u""")
conn.setencoding(encoding='utf-8')
conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')

main_query1 = f"SELECT {', '.join(features)} FROM \"freMPL-R\" WHERE gender = 'Male';"
df1 = pd.read_sql(main_query1, conn)

main_query2 = f"SELECT {', '.join(features)} FROM \"freMPL-R\" WHERE gender = 'Female';"
df2 = pd.read_sql(main_query2, conn)
##################################################


report_builder = HomogeneityReport(config_dict)
report = report_builder.build_report(df1, df2,
                                     name1='male', name2='female',
                                     render=False)

for feat_report in report:
    tests_res = feat_report[-1]
    for test in tests_res:
        conclusion = test['conclusion']
        if (conclusion == 'Different distributions') or (conclusion == 'Small difference'):
            print(f"{feat_report[0]} - {test['test']} test showed data drift. Conclusion = '{conclusion}'")

