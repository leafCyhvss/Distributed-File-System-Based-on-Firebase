import json
import requests
import pandas as pd
import warnings

warnings.filterwarnings("ignore")


class EDFSURL():
    def __init__(self) -> None:
        print('>>>EDFS Is Running')
        self.url = 'https://ds551-ad195-default-rtdb.firebaseio.com/'
        self.actualData = self.url + 'actualData/'
        self.rootPath = self.url + 'metadata/'
        self.currentPath = '/'

    def checkValidPath(self, filePath: str) -> bool:
        currPath = self.rootPath
        if filePath == '':
            return True
        if filePath[0] != '/':
            return False
        if filePath == '/':
            return True
        dirNames = filePath.split('/')[1:]  # 这里是为了去除 /user/john/hello.txt 第一个斜杠的影响，mac上不写这个斜杠
        for idx in range(len(dirNames)):
            dir = dirNames[idx]
            if dir == 'Empty':
                continue
            existedPaths = requests.get(currPath[:-1] + '.json').json()
            if dir not in existedPaths:
                # print('ERROR: Wrong Path: ' + currPath.replace(self.rootPath, '/') + dir)
                return False
            else:
                currPath += dir + '/'
        return True

    def put(self, filePath: str, systemPath: str, K: int) -> list:
        if systemPath[-1] == '/':
            systemPath = systemPath[:-1]

        dataset = pd.read_csv(filePath)
        if not self.checkValidPath(systemPath):
            print('Write ERROR: Wrong path: ' + systemPath)
            return ['Write ERROR: Wrong path: ' + systemPath]

        fileName = filePath.split('/')[-1].replace('.', '__')
        existedPaths = requests.get(self.rootPath[:-1] + systemPath + '.json').json()
        if fileName in existedPaths:
            print('Write ERROR: File ' + systemPath + '/' + fileName.replace('__', '.') + ' already exists')
            return ['Write ERROR: File ' + systemPath + '/' + fileName.replace('__', '.') + ' already exists']

        metaDict = {}
        for k in range(1, K + 1):
            metaDict['p' + str(k)] = self.actualData + systemPath.replace('/', '_') + '_' + fileName + 'p' + str(
                k) + '.json'
        # print(metaDict)
        newFileURL = self.rootPath[:-1] + systemPath + '/' + fileName + '.json'
        requests.put(newFileURL, data=json.dumps(metaDict))

        maxIndex = dataset.index.values[-1] // 1000
        actualData = []
        cnt = 0
        for k in range(K):
            jsondata = []
            if k != K - 1:
                for idx in range(cnt, cnt + maxIndex // K):
                    jsondata.append(dataset.iloc[idx].to_dict())
                actualData.append(jsondata)
            else:
                for idx in range(cnt, maxIndex + 1):
                    jsondata.append(dataset.iloc[idx].to_dict())
                actualData.append(jsondata)
            cnt += maxIndex // K
        for k in range(K):
            url = metaDict['p' + str(k + 1)].replace('.json', '/')
            for idx in range(len(actualData[k])):
                requests.put(url + str(idx) + '.json', data=json.dumps(actualData[k][idx]))
        print('Write: success')
        return ['Write: success']

    def remove(self, filePath: str) -> list:
        if filePath == '/':
            requests.delete(self.actualData[:-1] + '.json')
            return ['Remove ERROR: Should not remove root']
        if filePath[-1] == '/':
            filePath = filePath[:-1]
        if '.' in filePath:
            filePath = filePath.replace('.', '__')
        else:
            print('Remove ERROR: Must remove a file but not a directory')
            return ['Remove ERROR: Must remove a file but not a directory']
        if not self.checkValidPath(filePath):
            if '__' in filePath:
                filePath = filePath.replace('__', '.')
            print('Remove ERROR: Wrong path ' + filePath)
            return ['Remove ERROR: Wrong path ' + filePath]

        actualPaths = requests.get(self.rootPath[:-1] + filePath + '.json').json()
        for key, value in actualPaths.items():
            requests.delete(value)
        requests.delete(self.rootPath[:-1] + filePath + '.json')
        print('Remove: success')
        return ['Remove: success']

    def ls(self, filePath: str) -> list:
        filePath = filePath.replace(' ', '')
        if filePath == '':
            return {'success': 'ls success', 'data': ['/']}
        if filePath[-1] == '/':
            filePath = filePath[:-1]
        if '.' in filePath:
            print('Ls ERROR: Wrong path or try to use ls command to look into a file')
            return {'success': ['ls ERROR: Wrong path or try to use ls command to look into a file'],\
                    'data': ['Not A Directory']}

        if not self.checkValidPath(filePath):
            print('Ls ERROR: Wrong path ' + filePath)
            return {'success': 'ls ERROR: Wrong path ' + filePath, 'data': ['Wrong Path']}

        if filePath == '/':
            newPathURL = self.rootPath[:-1] + '.json'
        else:
            newPathURL = self.rootPath[:-1] + filePath + '.json'
        data = requests.get(url=newPathURL).json()
        response = []
        for name in data.keys():
            if len(data.keys()) == 1:
                print("This directory is empty")
                return {'success': "This directory is empty", 'data': ['This Directory is Empty']}
            if name == 'Empty':
                continue
            if '__' in name:
                name = name.replace('__', '.')
            print(filePath + '/' + name)
            response.append(filePath + '/' + name)
        return {'success': 'ls success', 'data': response}

    def cat(self, filePath: str) -> pd.DataFrame:
        dataset = pd.DataFrame()
        if '.' not in filePath:
            print('Cat ERROR: Wrong File Path' + filePath)
            return ['Cat ERROR: Wrong File Path' + filePath]
        filePath = filePath.replace('.', '__')
        if not self.checkValidPath(filePath):
            print('Cat ERROR: Wrong File Path' + filePath)
            return ['Cat ERROR: Wrong File Path' + filePath]
        actualPaths = requests.get(self.rootPath[:-1] + filePath + '.json').json()
        for key, value in actualPaths.items():
            records = requests.get(value).json()
            # 这里的 records 是一个列表，列表里是字典
            for record in records:
                dataset = dataset.append(record, ignore_index=True)
        print(dataset.info())
        return dataset.head()


    def get(self, filePath: str) -> pd.DataFrame:
        dataset = pd.DataFrame()
        if '.' not in filePath:
            print('Get File ERROR: Wrong File Path' + filePath)
            return ['Get File ERROR: Wrong File Path' + filePath]
        filePath = filePath.replace('.', '__')
        if not self.checkValidPath(filePath):
            print('Get File ERROR: Wrong File Path' + filePath)
            return ['Get File ERROR: Wrong File Path' + filePath]
        actualPaths = requests.get(self.rootPath[:-1] + filePath + '.json').json()
        for key, value in actualPaths.items():
            records = requests.get(value).json()
            # 这里的 records 是一个列表，列表里是字典
            for record in records:
                dataset = dataset.append(record, ignore_index=True)
        print('Get File: file stored in ./downloaded' + filePath.replace('__', '.').replace('/', '-'))
        dataset.to_csv('./downloaded' + filePath.replace('__', '.').replace('/', '-'))
        return dataset

    def mkdir(self, dirPath: str) -> list:
        '''
        mkdir一次只能创造一级文件夹
        '''
        if dirPath == '/':
            print('Mkdir ERROR: Path exists')
            return ['Mkdir ERROR: Directory Already Exists: ' + '/']
        if dirPath[-1] == '/':
            dirPath = dirPath[:-1]
        dirNames = dirPath.split('/')[1:]  # 这里是为了去除 /user/john/hello.txt 第一个斜杠的影响
        currPath = self.rootPath
        for idx in range(len(dirNames)):
            dir = dirNames[idx]
            if idx != len(dirNames) - 1:
                existedPaths = requests.get(currPath[:-1] + '.json').json()
                if dir not in existedPaths:
                    print('Mkdir ERROR: Wrong Path: ' + currPath.replace(self.rootPath, '/') + dir)
                    return ['Mkdir ERROR: Wrong Path: ' + currPath.replace(self.rootPath, '/') + dir]
                else:
                    currPath += dir + '/'
            else:
                existedPaths = requests.get(currPath[:-1] + '.json').json()
                if existedPaths and dir in existedPaths:
                    print('Mkdir ERROR: Directory Already Exists')
                    return ['Mkdir ERROR: Directory Already Exists: ' + dirPath]
                else:
                    dirURL = currPath + dir + '/Empty.json'
                    requests.put(url=dirURL, data=json.dumps('1'))
                    print('Mkdir: Success')
                    return ['Mkdir: Success']

    def getPartitionLocations(self, filePath) -> list:
        if '.' in filePath:
            filePath = filePath.replace('.', '__')
        else:
            print('Get Locations ERROR: Must query a file but not a directory')
            return ['Get Locations ERROR: Must query a file but not a directory']
        if not self.checkValidPath(filePath):
            if '__' in filePath:
                filePath = filePath.replace('__', '.')
            print('Get Locations ERROR: Wrong path ' + filePath)
            return ['Get Locations ERROR: Wrong path ' + filePath]

        actualPaths = requests.get(self.rootPath[:-1] + filePath + '.json').json()
        dataNodes = []
        for key, value in actualPaths.items():
            partNum = key[1:]
            dataNode = value.replace(self.actualData, '').replace('.json', '')
            dataNodes.append(dataNode)
            print('Get Locations: The %s part of the file is stored in datanode %s' % (partNum, dataNode))
        return dataNodes

    def readPartition(self, filePath, partition) -> pd.DataFrame:
        if '.' in filePath:
            filePath = filePath.replace('.', '__')
        else:
            print('Read Partition ERROR: Must read a file but not a directory')
            return ['Read Partition ERROR: Must read a file but not a directory']
        if not self.checkValidPath(filePath):
            if '__' in filePath:
                filePath = filePath.replace('__', '.')
            print('Read Partition ERROR: Wrong path ' + filePath)
            return ['Read Partition ERROR: Wrong path ' + filePath]

        dataset = pd.DataFrame()
        actualPaths = requests.get(self.rootPath[:-1] + filePath + '.json').json()
        number = 'p' + str(partition)
        if number not in actualPaths.keys():
            print('Read Partition ERROR: Wrong partition number ' + str(partition))
            return ['Read Partition ERROR: Wrong partition number ' + str(partition)]
        records = records = requests.get(actualPaths[number]).json()
        for record in records:
            dataset = dataset.append(record, ignore_index=True)
        print('Read Partition: The %s part of the file is:' % partition)
        print(dataset.head())
        print(dataset.info())
        return dataset
