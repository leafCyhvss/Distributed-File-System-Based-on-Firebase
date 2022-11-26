from django.shortcuts import render, redirect
from EDFS2.EDFS2 import EDFSURL


# Create your views here.
def helloworld(request):
    # 获取数据信息
    # edfs = EDFSURL()
    # filePaths = edfs.ls('/test1')
    # print(filePaths)
    return render(request, 'ls.html', {'queryset': ['/']})

def mainView(request):
    return render(request, 'lspost.html')

def lsDisplay(request):
    if request.method == 'GET':
        root = ['/']
        return render(request, 'lspost.html',{'queryset': root})
    if request.method == 'POST':
        edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        filePaths = edfs.ls(requestPath)
        print(filePaths)
        return render(request, 'ls.html', {'queryset': filePaths})

def catDisplay(request):
    if request.method == 'GET':
        return render(request, 'partpost.html')
    if request.method == 'POST':
        edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        filePaths = edfs.cat(requestPath)
        print(filePaths)
        return render(request, 'ls.html', {'queryset': filePaths})

def showPartition(request):
    if request.method == 'GET':
        return render(request, 'partpost.html')
    if request.method == 'POST':
        edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/' # 判断空
        filePaths = edfs.getPartitionLocations(requestPath)
        print(filePaths)
        return render(request, 'getparttionsloc.html', {'queryset': filePaths})

def mkdir(request):
    if request.method == 'GET':
        return render(request, 'makdir-request.html')
    else:
        edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        result = edfs.mkdir(requestPath)
        print(result)
        filePaths = edfs.ls(requestPath)
        return render(request, 'ls.html', {'queryset': filePaths})

def put(request):
    if request.method == 'GET':
        return render(request, 'put-request.html')
    else:
        edfs = EDFSURL()
        filePath = request.POST.get('filepath')
        pnumber = int(request.POST.get('pnumber'))
        print(request.FILES)
        fileObject = request.FILES.get('filename')
        print(fileObject)
        dataset = open(fileObject.name, 'wb')
        for chunk in fileObject.chunks():
            dataset.write(chunk)
        dataset.close()
        result = edfs.put(fileObject.name, filePath, pnumber)
        print(result)
        files = edfs.ls(filePath)
        return render(request, 'ls.html', {'queryset': files})
    # 考虑重定向去原来的页面，这样url会变