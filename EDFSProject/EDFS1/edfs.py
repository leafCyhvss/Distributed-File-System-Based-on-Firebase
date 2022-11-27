import json
import string

from firebase import firebase

# -------------------firebase-------------------

url = 'https://ds551-f9b92-default-rtdb.firebaseio.com/'
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
        print("files do not exist")
        return False
    tem = 1
    r = []
    for i in range(1, k + 1):
        r.append([])
    for line in f:
        if tem:
            c = line
            tem = 0
            continue
        ss = str(dict(zip(c.replace("\n", '').split(
            ","), line.replace("\n", '').split(','))))
        h = str(abs(hash(line)) % k + 1)
        r[int(h) - 1].append(ss)
    data = {}
    for i in range(1, k + 1):
        data[i] = r[i - 1]

    result = {}
    result['data'] = data
    par = {}
    for i in range(1, k + 1):
        par["p" + str(i)] = "https://ds551-f9b92-default-rtdb.firebaseio.com/data/" + file.split(".")[0] + str(
            i) + ".json"
    result["part"] = par
    return result


# print(readData("toyota.csv", 2))


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
        return {'success': True, "data": l}
    # 路径错误
    else:
        print('path is false')
        return {'success': False, 'data': ['Wrong']}


# print(ls("/user/yyy/"))


def mkdir(path, folder):
    # 新建文件路径
    pp = analysePath(path)
    # 验证
    if '~' in folder:
        print("folder illegal")
        return False
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
            print("folder exists")
            return False

        s = 'a' + s + "['" + folder + "']" + '={"file~":0}'
        exec(s)
        fb.put('test2', 'root', a)
        print('mkdir is success')
    # # 路径错误
    else:
        print('path is false')
        return False


# mkdir("/user", "ldw")

def cat(path, fileName):
    if path[-1] != "/":
        path = path+'/'
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print('path is false')
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
            ll = list(a.values())
            # print(path)
            for i in range(1, len(ll) + 1):
                f = fb.get("data", path.replace('/', '-') +
                           fileName.split(".")[0] + str(i))
                print(f)
                # print(path.replace('/','-')+fileName.split(".")[0] + str(i))
            return True
        else:
            print("file does not exist")
            return False


# cat("/user/yyy", 'toyota.csv')

def put(path, fileName, k):
    if path[-1] == '/':
        path = path[0:-1]
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print('path is false')
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
            print("file exists")
            return False
        else:

            ee = path.split("/")
            ww = ""
            for i in ee:
                ww = ww + i + "-"

            r = readData(fileName, k)
            if r == False:
                return
            for i in range(1, k + 1):
                pass
                # put data
                fb.put("data", ww + fileName.split(".")
                       [0] + str(i), r['data'][i])
            pass
            # update path
            s = ""
            pp['data'][-1] = pp['data'][-1].replace(".", "~")
            for p in pp['data']:
                print(p)
                s = s + '["' + p + '"]'

            a = getData()
            s = 'a' + s + '=' + str(r['part'])
            exec(s)
            fb.put('test2', 'root', a)
            return True


# print(put("/user/yyy", 'toyota.csv',8))


def readPartition(path, fileName):
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print('path is false')
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
            print()
            return list(a.keys())
        else:
            print("file does not exist")
            return False


# print(readPartition("/user/yyy", 'toyota.csv'))


def rm(path, fileName):
    fileName = fileName.replace(".", "~")
    a = getData()
    s = ''
    pp = analysePath(path)
    if not pp['success']:
        print('path is false')
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
            exec(s)
            fb.put('test2', 'root', a)

            print('rm success')
            return True
        else:
            print("file does not exist")
            return False


# print(rm("/user/yyy", 'toyota.csv'))


def getPartitionLocations(path, fileName, k):
    if path[-1] != "/":
        path = path + '/'
    a = getData()
    pp = analysePath(path)
    if not pp['success']:
        print('path is false')
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
                print("PartitionLocations does not exist")
                return False
            f = fb.get("data", path.replace('/', '-') +
                       fileName.split(".")[0] + str(k))
            print(f)
            return True
        else:
            print("file does not exist")
            return False


# print(getPartitionLocations("/user/yyy", 'toyota.csv',"1"))

def cmd(c):
    c = c.replace("  ", " ")
    c = c.strip()
    if " " not in c:
        return {'success': False, "data": "command not found"}

    cc = ['ls', 'mkdir', 'put', 'rm', 'cat',
          'getPartitionLocations', 'readPartition']
    a = c.split(" ")[0]
    if a not in cc:
        return {'success': False, "data": "command not found"}

    if a == 'ls':
        if len(c.split(" ")) != 2:
            return {'success': False, "data": "args not found"}
        else:
            return ls(c.split(" ")[1])

    if a == "mkdir":
        if len(c.split(" ")) != 3:
            return {'success': False, "data": "args not found"}
        else:
            return mkdir(c.split(" ")[1], c.split(" ")[2])

    if a == "put":
        if len(c.split(" ")) != 4:
            return {'success': False, "data": "args not found"}
        else:
            return put(c.split(" ")[1], c.split(" ")[2], int(c.split(" ")[3]))

    if a == 'rm':
        if len(c.split(" ")) != 3:
            return {'success': False, "data": "args not found"}
        else:
            if c.split(" ")[1][-1] == '/':

                return rm(c.split(" ")[1][0:-1], c.split(" ")[2])
            else:
                return rm(c.split(" ")[1], c.split(" ")[2])

    if a == 'cat':
        if len(c.split(" ")) != 3:
            return {'success': False, "data": "args not found"}
        else:
            return cat(c.split(" ")[1], c.split(" ")[2])

    if a == 'getPartitionLocations':
        if len(c.split(" ")) != 4:
            return {'success': False, "data": "args not found"}
        else:
            return getPartitionLocations(c.split(" ")[1], c.split(" ")[2], c.split(" ")[3])

    if a == 'readPartition':
        if len(c.split(" ")) != 3:
            return {'success': False, "data": "args not found"}
        else:
            return readPartition(c.split(" ")[1], c.split(" ")[2])


# print(cmd("readPartition /user/yyy/ toyota.csv "))

if __name__ == '__main__':
    print("welcome EDFS~")
    tem = True
    while tem:
        c = input()
        if c == 'exit':
            tem = False
            print("bye~")
        # print(cmd("readPartition /user/yyy/ toyota.csv "))
        if c != 'exit':
            print(cmd(c))
