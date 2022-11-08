import pymysql
from sshtunnel import SSHTunnelForwarder
import pandas as pd

# 为了兼容
pymysql.install_as_MySQLdb()




dataset = pd.read_csv('./audi.csv')
print(dataset.head())

columns = dataset.columns.values
createTableSql = 'create table audi (model varchar(20), year int, price int, transmission varchar(20), mileage varchar(20),\
    fueltype varchar(20), tax int, mpg float, enginSize float)'
dataset = pd.read_csv('./audi.csv')
maxIndex = dataset.index.values[-1]
jsonData = []
for idx in range(maxIndex):
    jsonData.append(dataset.iloc[idx].to_dict())


insertIntoSql = 'insert into audi(model, year) values(%s,%s)'
insertData = list(zip(dataset.iloc[:10]['model'],dataset.iloc[:10]['year']))
print(insertData)
with SSHTunnelForwarder(
        ('ec2-54-215-95-69.us-west-1.compute.amazonaws.com', 22),  # 跳板机（堡垒机）B配置
        ssh_pkey='./project.pem',
        ssh_username='ec2-user',
        remote_bind_address=('127.0.0.1', 3306)) as server:  # 数据库存放服务器C配置

    # 打开数据库连接
    db_connect = pymysql.connect(host='127.0.0.1',   # 本机主机A的IP（必须是这个）
                                 port=server.local_bind_port, 
                                 user='root',
                                 passwd='Dsci-551',
                                 db='test')  # 需要连接的数据库的名称
    cursor = db_connect.cursor()

    try:
        # 执行SQL语句
        # cursor.execute(createTableSql)
        # 获取所有记录列表

        cursor.executemany(insertIntoSql,insertData)
        results = cursor.fetchall()
        print(results)
    except Exception as data:
        print('Error: 执行查询失败，%s' % data)

    db_connect.close()


