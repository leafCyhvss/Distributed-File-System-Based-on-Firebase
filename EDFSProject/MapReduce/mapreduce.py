from EDFS2.EDFS2 import EDFSURL
from EDFS1 import edfs

class MapReduce():

    def __init__(self):
        self.edfs2 = EDFSURL()
    def searchMapper(self, filepath, edfsType):
        ans = {}
        if edfsType == '2':
            result = self.edfs2.getPartitionLocations(filepath)
            if result['success'] == ['Get Locations Success ']:
                for datanode in (result['data']):
                    dataset = self.edfs2.getFilebyDatanode(datanode)
                    # 找出某个条件的数据行
                    # 数据行 存df
                    # ans{filepath:df}

        return
        # {toyota.csv: df1,df2,df3}
        # 根据文件路径，获取分区数量
        # 大for循环
            # 对每个分区的文件分别下载
            # 对每个分区的处理
            # 结果[filename, value]保存到结果ans中



    def sumReducer(self, k,v):
        # df
        # key
        # DF=df1+df2+
        # return DF


    # analyse
    #     9 3*3
    #     ford to 2*1
