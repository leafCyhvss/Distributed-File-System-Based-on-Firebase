from EDFS2.EDFS2 import EDFSURL
from EDFS1 import edfs

class MapReduce():

    def __init__(self):
        self.edfs2 = EDFSURL()
    def searchMapper(self, filepaths, edfsType):
        # [{toyota.csv: df1},{toyota.csv: df2},{audi.csv: df3}]
        # 根据文件路径，获取分区数量
        # 大for循环
            # 对每个分区的文件分别下载
            # 对每个分区的处理
            # 结果[filename, value]保存到结果ans中
        ans = []
        if edfsType == '2':
            for filepath in filepaths:
                result = self.edfs2.getPartitionLocations(filepath)
                fileName = filepath.split('/')[-1]
                # print(result)
                if result['success'] == ['Get Locations Success ']:
                    for datanode in (result['data']):
                        dataset = self.edfs2.getFilebyDatanode(datanode)
                        # 找出某个条件的数据行
                        # 数据行 存df
                        searchedDf = dataset.loc[(dataset['price']>12000)\
                                                 &(dataset['transmission'] == 'Manual')]
                        if len(searchedDf.index) != 0:
                            ans.append({fileName: searchedDf})
            print('ans\n', ans)
            return ans
        if edfsType == '1':
            pass



    def searchReducer(self, keyValuePair):
        '''
        # key
        # DF=df1+df2+
        # return DF# analyse#     9 3*3#     ford to 2*1
        :param keyValuePair:
        :return:
        '''
        import collections
        dataset = collections.defaultdict(lambda: [])
        for dic in keyValuePair:
            fileName = list(dic.keys())[0]
            dataset[fileName].append(list(dic.values())[0])
        for key, value in dataset.items():
            for partData in value:



x = MapReduce()
ans = x.searchMapper(['/test1/audi.csv', '/test1/toyota.csv'], '2')
x.searchReducer(ans)