import json
import string

from firebase import firebase
import pandas as pd

# -------------------firebase-------------------

url = 'https://demo01-76e03-default-rtdb.firebaseio.com/'
fb = firebase.FirebaseApplication(url, None)


# -------------------firebase-------------------

# fb.put('test2', 'root', {'user': {'ldw2': {'file~': 0}, 'ldw': {'aa~txt': {'p1': "c1.json", 'p2': 'c2.json'},
#                                                                 'ww~txt': {'p1': "ww1.json", 'p2': 'ww2.json'},
#                                                                 'file~': 1},
#                                   'lcj': {'l~txt': {'p1': "l1.json", 'p2': 'l2.json'}, 'file~': 1}}})


def getData():
    return fb.get('test2', "root")


def readData(file, k):
    try:
        f = open(file, 'r', encoding="utf-8")
    except:
        print("文件不存在")
        return False
    tem = 1
    r = []
    count = 0
    result = {}
    for i in range(1, k + 1):
        # r.append([])
        result[(i)] = []
    for line in f:
        count = count + 1
        # if tem:
        #     c = line
        #     tem = 0
        #     print(c)
        #     continue
        # print(abs(hash(line)))
        result[(abs(hash(line)) % k + 1)].append({'data': line, "index": count})
    # print(result)
    #     ss = str(dict(zip(c.replace("\n", '').split(","), line.replace("\n", '').split(','))))
    #     h = str(abs(hash(line)) % k + 1)
    #     r[int(h) - 1].append(ss)
    # data = {}
    # for i in range(1, k + 1):
    #     data[i] = r[i - 1]
    # result['data'] = data
    par = {}
    for i in range(1, k + 1):
        par["p" + str(i)] = "https://demo01-76e03-default-rtdb.firebaseio.com/data/" + file.split(".")[0] + str(
            i) + ".json"
    result["part"] = par

    # print(result)
    return result


# readData("toyota.csv", 2)


# -------------------firebase-------------------
# to end
def analysePath(path):
    pp = []
    a = getData()

    # 删除最后的/
    if path[-1] == '/':
        path = path[0:-1]

    # 路径开头必须为/
    if path == "/":
        return {'success': True, "data": ['/']}

    if path[0] != '/':
        return {'success': False}
    else:

        # 拆路径
        for p in path.split("/")[1:]:

            # 获取数据路径，不含 ~（~代表文件）
            dd = list(a.keys())
            ee = []
            for d in dd:
                if '~' not in d:
                    ee.append(d)

            if p not in ee:
                return {'success': False}
            else:
                pp.append(p)
                a = a[p]
                if not isinstance(a, dict):
                    break

        # 路径超长
        if len(pp) < len(path.split("/")[1:]):
            return {'success': False}

        return {'success': True, "data": pp}


# print(analysePath("/user/ldw"))


def ls(path):
    if path == '':
        return {'success': 'ls success', 'data': ['/']}

    if '.' in path:
        print('Ls ERROR: Wrong path or try to use ls command to look into a file')
        return {'success': ['ls ERROR: Wrong path or try to use ls command to look into a file'], \
                'data': ['Not A Directory']}
    a = getData()
    pp = analysePath(path)

    if pp['success']:
        for p in pp['data']:
            c = a[p]
            a = a[p]
        l = []
        # 含~ 为文件  file~:0 无文件
        for k in list(c.keys()):
            if '~' in k and k != 'file~':
                l.append(k)
        if l == []:
            print("No file")
            return {'success': "This directory is empty", 'data': ['This Directory is Empty']}

        for i in l:
            i = i.replace("~", ".")
        for index in range(len(l)):
            l[index] = l[index].replace("~", ".")
        # print(l)
        return {'success': 'ls success', 'data': l}

    # 路径错误
    else:
        print('Ls ERROR: Wrong path: ' + path)
        return {'success': 'ls ERROR: Wrong path ' + path, 'data': ['Wrong Path']}


# print(ls("/user/yyy/"))


def mkdir(filepath):
    if filepath[-1] == '/':
        filepath = filepath[:-1]
    folder = filepath.split('/')[-1]
    print('folder', folder)
    path = '/'.join(filepath.split('/')[:-1])
    print('path', path)
    if path == '/':
        print('Mkdir ERROR: Path exists')
        return ['Mkdir ERROR: Directory Already Exists: ' + '/']
    # 新建文件路径
    pp = analysePath(path)
    # 验证
    if '~' in folder:
        print("LS ERROR:Folder wrong")
        return ['Mkdir ERROR: Directory Already Exists: ' + '/']
    # 获取数据
    a = getData()
    c = a
    s = ''
    # 路径ok
    if pp['success']:
        for p in pp['data']:
            c = c[p]
            s = s + '["' + p + '"]'
        c_key = list(c.keys())

        if folder in c_key:
            print("LS ERROR:Folder wrong")
            return ['Mkdir ERROR: Directory Already Exists: ' + path]
        print(s)
        s = 'a' + s + "['" + folder + "']" + '={"empty":1}'
        # s = 'a' + s + "['" + folder + "']" + '={"file~":0}'
        # print(a)
        exec(s)
        # exec (i)
        fb.put('test2', 'root', a)
        print('mkdir success')
        return ['Mkdir: Success']
    # # 路径错误
    else:
        print("LS ERROR:Folder wrong")
        return ['Mkdir ERROR: Wrong Path: ' + path]


# mkdir("/user", "ldw4")


def cat(filepath):
    if filepath[-1] == '/':
        filepath = filepath[:-1]
    fileName = filepath.split('/')[-1]
    print('fileName', fileName)
    path = '/'.join(filepath.split('/')[:-1])
    print('path', path)

    if path[-1] != "/":
        path = path + '/'
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print('path is false')
        return {'success': ['Cat ERROR: Wrong File Path' + path], 'data': 'Incorrect Input'}
    else:
        tem = True
        c = pp['data']
        c.append(fileName.replace(".", "~"))

        for p in c:
            e = list(a.keys())
            if p in e:
                a = a[p]
            else:
                tem = False
        if tem:
            ll = list(a.values())
            data = {}
            # print(path)
            for i in range(1, len(ll) + 1):
                f = fb.get("data", path.replace('/', '-') + fileName.split(".")[0] + str(i))
                # print(f)

                for d in f:
                    data[d['index']] = d['data']
            columns = data[1].split(',')

            datas = []
            for i in range(2, len(data) + 1):
                datas.append(data[i].replace('\n', '').split(','))
                # print(data[i])

            # pd.set_option('display.max_columns', None)
            # # 显示所有行
            # pd.set_option('display.max_rows', None)
            df = pd.DataFrame(datas,
                              columns=columns)
            # df.head()
            # 显示所有列
            pd.set_option('display.max_columns', None)
            # 显示所有行
            pd.set_option('display.max_rows', None)
            print(df.head())
            print(df.info())
            return {'success': ['Cat Success'], 'data': df}
        else:
            print("Cat ERROR:File Name wrong")
            return {'success': ['Cat ERROR: Wrong File Path' + path], 'data': 'Incorrect Input'}


# cat("/user/lcj", 'toyota.csv')

def get(path, fileName):
    if '.' not in fileName:
        print('Get File ERROR: Wrong File Path' + path)
        return ['Get File ERROR: Wrong File Path: ' + path]
    if path[-1] != "/":
        path = path + '/'
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print('path is false')
        return ['Get File ERROR: Wrong File Path: ' + path]
    else:
        tem = True
        c = pp['data']
        c.append(fileName.replace(".", "~"))

        for p in c:
            e = list(a.keys())
            if p in e:
                a = a[p]
            else:
                tem = False
        if tem:
            ll = list(a.values())
            data = {}
            # print(path)
            for i in range(1, len(ll) + 1):
                f = fb.get("data", path.replace('/', '-') + fileName.split(".")[0] + str(i))
                # print(f)

                for d in f:
                    data[d['index']] = d['data']
            fg = open("copy_" + fileName, "a")
            for i in range(1, len(data) + 1):
                fg.write(data[i])
            print("save done")
            fg.close()
            return data
        else:
            print("文件已不存在")
            return ['Get File ERROR: Wrong File Path: ' + path]


# get("/user/lcj", 'toyota.csv')

def put(path, fileName, k):
    if path[-1] == '/':
        path = path[0:-1]
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print("Put ERROR:path wrong")
        return ['Write ERROR: Wrong path: ' + path]
    else:

        tem = True
        c = pp['data']
        c.append(fileName.replace(".", "~"))

        for p in c:
            e = list(a.keys())
            if p in e:
                a = a[p]
            else:
                tem = False

        if tem:
            print("Put ERROR:path wrong")
            return ['Write ERROR: File ' + path + '/' + ' already exists']
        else:

            ee = path.split("/")
            ww = ""
            for i in ee:
                ww = ww + i + "-"

            r = readData(fileName, k)

            if r == False:
                return False;
            for i in range(1, k + 1):
                # put data
                fb.put("data", ww + fileName.split(".")[0] + str(i), r[i])

            # update path
            s = ""
            c = ""
            pp['data'][-1] = pp['data'][-1].replace(".", "~")
            for p in pp['data']:
                s = s + '["' + p + '"]'
                if '~' not in p:
                    c = c + '["' + p + '"]'
            a = getData()

            s = 'a' + s + '=' + str(r['part'])
            c = 'a' + c + '["empty"] = "0"'

            exec(s)
            exec(c)
            fb.put('test2', 'root', a)
            print("Put success")
            return ['Write: success']


# print(put("/user/ldw/", 'cars.csv',2))


def getPartitionLocations(filepath):
    if filepath[-1] == '/':
        filepath = filepath[:-1]
    fileName = filepath.split('/')[-1]
    print('fileName', fileName)
    path = '/'.join(filepath.split('/')[:-1])
    print('path', path)
    # if '.' not in path:
    #     return {'success': ['Read Partition ERROR: Must read a file but not a directory'], 'data': 'Incorrect input'}

    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print("ReadPartition ERROR:path wrong")
        return {'success': ['Read Partition ERROR: Wrong path ' + path], 'data': ['Incorrect input']}

    else:

        tem = True
        c = pp['data']
        c.append(fileName.replace(".", "~"))

        for p in c:
            e = list(a.keys())
            if p in e:
                a = a[p]
            else:
                tem = False
        if tem:
            # print(a.keys())
            index = 0
            for i in list(a.keys()):
                index = index + 1
                print("Get Location The " + str(index) + " part of the file in stored in datanode " + i)
            return {'success': ['Get Locations Success '], \
                    'data': a.keys()}
        else:
            return {'success': ['Read Partition ERROR: Wrong path ' + path], 'data': ['Incorrect input']}



# readPartition("/user/yyy", 'toyota.csv')


def rm(filepath):
    if filepath[-1] == '/':
        filepath = filepath[:-1]
    fileName = filepath.split('/')[-1]
    print('fileName', fileName)
    path = '/'.join(filepath.split('/')[:-1])
    print('path', path)

    fileName = fileName.replace(".", "~")
    a = getData()
    s = ''
    pp = analysePath(path)
    if not pp['success']:
        print("RM ERROR:Path wrong")
        return ['Remove ERROR: Wrong path or file not exists ' + path]
    else:
        tem = True
        c = pp['data']
        c.append(fileName.replace(".", "~"))
        for p in c:
            e = list(a.keys())
            if p in e:
                a = a[p]
            else:
                tem = False
        if tem:

            ee = path.split("/")
            ww = ""
            for i in ee:
                ww = ww + i + "-"
            h = readPartition(path, fileName)
            for i in range(1, len(h) + 1):
                # print(ww + fileName.split('~')[0] + str(i))
                fb.delete("data", ww + fileName.split('~')[0] + str(i))

            for p in pp['data']:
                s = s + '["' + p + '"]'
            a = getData()
            s = "del a" + s
            print(s)
            exec(s)
            fb.put('test2', 'root', a)

            print('rm success')
            # print(path['data'])
            if len(ls(path)['data']) == 0:

                pp = analysePath(path)
                c = ""
                pp['data'][-1] = pp['data'][-1].replace(".", "~")
                for p in pp['data']:
                    if '~' not in p:
                        c = c + '["' + p + '"]'
                a = getData()
                c = 'a' + c + '["empty"] = "1"'
                exec(c)
                fb.put('test2', 'root', a)
            return ['Remove: success']
        else:
            print("RM ERROR:File Name wrong")
            return ['Remove ERROR: Wrong path or file not exists ' + path]


# print(rm("/user/ldw", 'copytoyota.csv'))


def readPartition(filepath, k):
    if filepath[-1] == '/':
        filepath = filepath[:-1]
    fileName = filepath.split('/')[-1]
    print('fileName', fileName)
    path = '/'.join(filepath.split('/')[:-1])
    print('path', path)
    if path[0] != '/':
        return {'success': ['Get Locations ERROR: Syntax start with /'], \
                'data': ['Incorrect Query']}
    if '.' not in fileName:
        return {'success': ['Get Locations ERROR: Must query a file but not a directory'], \
                'data': ['Incorrect Query']}

    if '~' in path:
        return {'success': ['Get Locations ERROR: Wrong path ' + path], \
                'data': ['Incorrect Query']}

    if path[-1] != "/":
        path = path + '/'
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        return {'success': ['Get Locations ERROR: Wrong path ' + path], \
                'data': ['Incorrect Query']}
        return False
    else:

        tem = True
        c = pp['data']
        c.append(fileName.replace(".", "~"))

        for p in c:
            e = list(a.keys())
            if p in e:
                a = a[p]
            else:
                tem = False

        if tem:
            t = True
            # print(a.keys())
            for a in list(a.keys()):
                # print(a)
                if str(k) in a:
                    t = False

            if t:
                print("RM GetPartitionLocations:PartitionLocations wrong")
                return {'success': ['Get Locations ERROR: Wrong path ' + path], \
                        'data': ['Incorrect Query']}
            f = fb.get("data", path.replace('/', '-') + fileName.split(".")[0] + str(k))
            dataset = pd.DataFrame()
            for i in f:
                #print(i)
                dataset = dataset.append(i, ignore_index=True)
            return {'success': ['Get Locations Success '], \
                    'data': dataset}

        else:
            return {'success': ['Get Locations ERROR: Wrong path ' + path], \
                    'data': ['Incorrect Query']}



# getPartitionLocations("/user/yyy", 'toyota.csv',"1")

# def cmd(c):
#     c = c.replace("  ", " ")
#     c = c.strip()
#     if " " not in c:
#         print("command not found")
#         return {'success': False, "data": "command not found"}
#
#     cc = ['ls', 'mkdir', 'put', 'rm', 'cat', 'getPartitionLocations', 'readPartition', 'get']
#     a = c.split(" ")[0]
#     if a not in cc:
#         print("command not found")
#         return {'success': False, "data": "command not found"}
#
#     if a == 'ls':
#         if len(c.split(" ")) != 2:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             return ls(c.split(" ")[1])
#
#     if a == "mkdir":
#         if len(c.split(" ")) != 3:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             return mkdir(c.split(" ")[1], c.split(" ")[2])
#
#     if a == "put":
#         if len(c.split(" ")) != 4:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             return put(c.split(" ")[1], c.split(" ")[2], int(c.split(" ")[3]))
#
#     if a == 'rm':
#         if len(c.split(" ")) != 3:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             if c.split(" ")[1][-1] == '/':
#
#                 return rm(c.split(" ")[1][0:-1], c.split(" ")[2])
#             else:
#                 return rm(c.split(" ")[1], c.split(" ")[2])
#
#     if a == 'cat':
#         if len(c.split(" ")) != 3:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             return cat(c.split(" ")[1], c.split(" ")[2])
#
#     if a == 'get':
#         if len(c.split(" ")) != 3:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             return get(c.split(" ")[1], c.split(" ")[2])
#
#     if a == 'getPartitionLocations':
#         if len(c.split(" ")) != 4:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             return getPartitionLocations(c.split(" ")[1], c.split(" ")[2], c.split(" ")[3])
#
#     if a == 'readPartition':
#         if len(c.split(" ")) != 3:
#             print("args not found")
#             return {'success': False, "data": "args not found"}
#         else:
#             return readPartition(c.split(" ")[1], c.split(" ")[2])
#
#
# # print(cmd("readPartition /user/yyy/ toyota.csv "))
#
# if __name__ == '__main__':
#     print("welcome EDFS~")
#     tem = True
#     while tem:
#         c = input()
#         if c == 'exit':
#             tem = False
#             print("bey~")
#         # print(cmd("readPartition /user/yyy/ toyota.csv "))
#         if c != 'exit':
#             cmd(c)
