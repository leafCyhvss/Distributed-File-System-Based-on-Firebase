import pandas as pd
import numpy as np                       #visualisation
import matplotlib.pyplot as plt             #visualisation
import seaborn as sns
from firebase import firebase

from EDFS2.EDFS2 import EDFSURL
from EDFS1 import edfs


class MapReducer():

    def __init__(self):
        self.edfs2 = EDFSURL()

    def searchMapper(self, filepaths, otherparam):
        # [{toyota.csv: df1},{toyota.csv: df2},{audi.csv: df3}]
        # 根据文件路径，获取分区数量
        # 大for循环
        # 对每个分区的文件分别下载
        # 对每个分区的处理
        # 结果[filename, value]保存到结果ans中
        ans = []
        edfsType = int(otherparam['edfsType'])
        price = int(otherparam['price'])
        trans = otherparam['trans']
        if edfsType == 2:
            for filepath in filepaths:
                result = self.edfs2.getPartitionLocations(filepath)
                fileName = filepath.split('/')[-1]
                # print(result)
                if result['success'] == ['Get Locations Success ']:
                    for datanode in (result['data']):
                        dataset = self.edfs2.getFilebyDatanode(datanode)
                        # 找出某个条件的数据行
                        # 数据行 存df
                        searchedDf = dataset.loc[(dataset['price'] > price) \
                                                 & (dataset['transmission'] == trans)]
                        if len(searchedDf.index) != 0:
                            ans.append({fileName: searchedDf})
            # print('ans\n', ans)
            return ans
        if edfsType == 1:
            data1 = edfs.cat(filepaths[0])['data']
            data1['price'] = pd.to_numeric(data1['price'])
            data1 = data1.loc[(data1['price'] > price) \
                                                 & (data1['transmission'] == trans)]
            data2 = edfs.cat(filepaths[1])['data']
            # data2['price'] = data2['price'].astype(int, errors="ignore")
            data2['price'] = pd.to_numeric(data2['price'])
            data2 = data2.loc[(data2['price'] > price) \
                                                 & (data2['transmission'] == trans)]
            return [{filepaths[0].split('/')[-1]:data1}, {filepaths[1].split('/')[-1]:data2}]


    def searchReducer(self, keyValuePair):
        '''
        :param keyValuePair: [k,v] result introduced in lecture
        :return:
        '''
        import collections
        tmpdataset = collections.defaultdict(lambda: [])
        finalData = collections.defaultdict(lambda: pd.DataFrame())
        for dic in keyValuePair:
            fileName = list(dic.keys())[0]
            tmpdataset[fileName].append(list(dic.values())[0])
        for fileName, DFlist in tmpdataset.items():
            for partData in DFlist:
                finalData[fileName] = pd.concat([finalData[fileName], partData], ignore_index=True)
        # print('='*50)
        # print(finalData)

        return finalData

    def analyseReducer(self, keyValuePair, method):
        dataset = pd.DataFrame()
        for dic in keyValuePair:
            for k,v in dic.items():
                dataset = pd.concat([dataset, v], ignore_index=True)
        #print(dataset)
        dataset.to_csv('./localData/output.csv')
        #k.split('.')[0]
        df = pd.read_csv('./localData/output.csv')
        # print(df['year'].corr(df['price']))
        # print(df.shape)
        if method == 'method1':
            df['year'].value_counts()
            df["age"] = 2022 - df['year']
            df["Current Value"] = df['price'] * (0.875) ** df["age"]
            audi = df.drop(df[(df['year'] < 2002) | (df['year'] > 2019) | (df['Current Value'] < 0)].index)
            fig, ax = plt.subplots(1, 1)
            sns.countplot(y='year', data=df, order=df['year'].value_counts()[0:10].index,
                          hue='transmission').tick_params(axis='x', rotation=90)
            plt.title("%s car production from 2017-2009 based on transmisson wise"%(k.split('.')[0]))
            fig.savefig('./static/img/analyseResult.png')
        if method == 'method2':
            fig, ax = plt.subplots(1, 1)
            sns.relplot(x="year", y="price", hue="transmission", kind="scatter", data=df)
            ax = sns.scatterplot(data=df, x="year", y="price", hue="transmission")
            sns.regplot(data=df, x="year", y="price", scatter=False, ax=ax)
            plt.title("Depreciation of %s cars at different prices when selling" %(k.split('.')[0]))
            fig.savefig('./static/img/analyseResult.png')

    # def analyseMapper(self, filepath, method):
    #     url = 'https://demo01-76e03-default-rtdb.firebaseio.com/'
    #     fb = firebase.FirebaseApplication(url, None)
    #     if filepath[-1] == '/':
    #         filepath = filepath[:-1]
    #     fileName = filepath.split('/')[-1]
    #     print('folder', fileName)
    #     path = '/'.join(filepath.split('/')[:-1])
    #     print('path', path)
    #     f = fb.get("data", path.replace('/', '-') + fileName.split(".")[0])
    #     dataset = pd.DataFrame()
    #     for i in f:
    #         print(i)
    #     dataset = dataset.append(i, ignore_index=True)
    #     print(dataset)
    def analyseMapper(self, filepath):
        if filepath[-1] == '/':
            filepath = filepath[:-1]
        fileName = filepath.split('/')[-1]
        # print('folder', fileName)
        path = '/'.join(filepath.split('/')[:-1])
        # print('path', path)
        dataset = pd.DataFrame()
        data = edfs.cat(filepath)['data']
        # print(type(data))
        # print(data)
        # for data_ in data:
        #     dataset = dataset.append(data_, ignore_index=True)
        # print(type(dataset))
        # print(data.info())
        return [{fileName: data}]

# x = MapReducer()
# ans = x.searchMapper(['/test1/audi.csv'], {'edfsType': 2, 'price': 17000,'trans':'Manual'})
# # x.analyseReducer(ans, 123)
# y = x.analyseMapper('/user/yyy/audi.csv')
# x.analyseReducer(y, 123)
