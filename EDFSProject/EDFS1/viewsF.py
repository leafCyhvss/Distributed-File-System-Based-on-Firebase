from django.shortcuts import render
from EDFS1 import edfs


# Create your views here.
def helloworld(request):
    return render(request, 'helloworld.html')


def edfs1Main(request):
    if request.method == 'GET':
        root = edfs.ls('/user')['data']
        return render(request, 'edfs1-ls.html', {'msg': 'All path start with /', 'path': 'root (/)', 'queryset': root})
    # render(request, 'edfs2-ls.html', {'msg': result[0], 'path': requestPath, 'queryset': filePaths}
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        ans = edfs.ls(requestPath)
        msg = ans['success']
        filePaths = ans['data']
        print(filePaths)
        return render(request, 'edfs1-ls.html', {'msg': msg,'path': requestPath, 'queryset': filePaths})


def lsDisplay(request):
    if request.method == 'GET':
        return render(request, 'edfs1-ls-request.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        ans = edfs.ls(requestPath)
        msg = ans['success']
        filePaths = ans['data']
        print(filePaths)
        return render(request, 'edfs1-ls.html',{'msg': msg,'path': requestPath, 'queryset': filePaths})


def catDisplay(request):
    if request.method == 'GET':
        return render(request, 'edfs1-cat-request.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        # requestPath = requestPath if requestPath else '/'
        ans = edfs.cat(requestPath)
        msg = ans['success']
        print('msg', msg)
        result = ans['data']
        if msg[0] == 'Cat Success':
            result = ans['data']
            # print(filePaths)
            # print(result)
            # pd.set_option('colheader_justify', 'center')
            return render(request, 'edfs1-cat-result.html', \
                          {'msg': msg[0], 'table': result.to_html(classes="table table-bordered table-hover")})
        else:
            return render(request, 'edfs1-cat-result.html', \
                          {'msg': msg[0], 'table': 'Incorrect input'})


def showPartition(request):
    if request.method == 'GET':
        return render(request, 'edfs1-locpart-reuqest.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'  # 判断空
        msg = edfs.getPartitionLocations(requestPath)['success'][0]
        filePaths = edfs.getPartitionLocations(requestPath)['data']
        print(filePaths)
        return render(request, 'edfs1-locpart-result.html', {'msg': msg, 'queryset': filePaths})


def mkdir(request):
    if request.method == 'GET':
        return render(request, 'edfs1-mkdir-reuqest.html')
    else:
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        result = edfs.mkdir(requestPath)
        print(result)
        filePaths = edfs.ls(requestPath)['data']
        return render(request, 'edfs1-ls.html', {'msg': result[0], 'path': requestPath, 'queryset': filePaths})


def put(request):
    if request.method == 'GET':
        return render(request, 'edfs1-put-request.html')
    else:
        # edfs = EDFSURL()
        filePath = request.POST.get('filepath')
        pnumber = int(request.POST.get('pnumber'))
        print(request.FILES)
        fileObject = request.FILES.get('filename')
        print(fileObject)
        dataset = open('./localData/' + fileObject.name, 'wb')
        for chunk in fileObject.chunks():
            dataset.write(chunk)
        dataset.close()
        result = edfs.put(filePath, './localData/' + fileObject.name, pnumber)
        print(result)
        files = edfs.ls(filePath)['data']
        return render(request, 'edfs1-ls.html', {'msg': result[0], 'path': filePath, 'queryset': files})
    # 考虑重定向去原来的页面，这样url会变
def remove(request):
    if request.method == 'GET':
        return render(request, 'edfs1-remove-request.html')
    if request.method == 'POST':
        requestPath = request.POST.get('title')
        print(requestPath)
        requestPath = requestPath if requestPath else '/'
        print('re:', requestPath)
        if requestPath == '/':
            files = edfs.ls('/')['data']
            return render(request, 'edfs1-ls.html', \
                          {'msg': 'Remove ERROR: Should not remove root (/)', \
                           'path': 'root (/)', \
                           'queryset': files})
        result = edfs.rm(requestPath)
        print(result)
        filePath = requestPath.split('/')[:-1]
        filePath = '/'.join(filePath)
        print('file path: ', filePath)
        files = edfs.ls(filePath)['data']
        return render(request, 'edfs1-ls.html', {'msg': result[0], 'path': filePath, 'queryset': files})

def readPart(request):
    if request.method == 'GET':
        return render(request, 'edfs1-readpart-request.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('filepath')
        # requestPath = requestPath if requestPath else '/'
        pnumber = request.POST.get('pnumber')

        ans = edfs.readPartition(requestPath, int(pnumber))
        msg = ans['success']
        result = ans['data']
        print('msg', msg)
        if msg[0] == 'Read Partition Success':
            # pd.set_option('colheader_justify', 'center')
            return render(request, 'edfs1-readpart-result.html', \
                          {'msg': msg[0], 'table': result.to_html(classes="table table-bordered table-hover")})
        else:
            return render(request, 'edfs1-cat-result.html', \
                          {'msg': msg[0], 'table': result.to_html(classes="table table-bordered table-hover")})

def analytics(request):
    return render(request, 'analytics.html')

def report(request):
    return render(request, 'report.html')
