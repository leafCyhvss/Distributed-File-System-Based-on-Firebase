from django.shortcuts import render, redirect
from EDFS2.EDFS2 import EDFSURL
from django.urls import reverse

edfs = EDFSURL()

# Create your views here.
def helloworld(request):
    # 获取数据信息
    # edfs = EDFSURL()
    files = edfs.ls(edfs.currentPath)
    print('helloworld' +
        edfs.currentPath)
    return render(request, 'edfs2-ls.html', {'queryset': files})

def mainView(request):
    return render(request, 'edfs2-lspost.html')

def lsDisplay(request):
    if request.method == 'GET':
        root = ['/']
        return render(request, 'edfs2-lspost.html', {'queryset': root})
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        filePaths = edfs.ls(requestPath)
        print(filePaths)
        return render(request, 'edfs2-ls.html', {'queryset': filePaths})

def catDisplay(request):
    if request.method == 'GET':
        return render(request, 'partpost.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        filePaths = edfs.cat(requestPath)
        print(filePaths)
        return render(request, 'edfs2-ls.html', {'queryset': filePaths})

def showPartition(request):
    if request.method == 'GET':
        return render(request, 'partpost.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/' # 判断空
        filePaths = edfs.getPartitionLocations(requestPath)
        print(filePaths)
        return render(request, 'edfs2-showpartloc.html', {'queryset': filePaths})

def mkdir(request):
    if request.method == 'GET':
        return render(request, 'makdir-request.html')
    else:
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        result = edfs.mkdir(requestPath)
        print(result)
        filePaths = edfs.ls(requestPath)
        return render(request, 'edfs2-ls.html', {'queryset': filePaths})

def put(request):
    if request.method == 'GET':
        return render(request, 'put-request.html')
    else:
        # edfs = EDFSURL()
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
        return render(request, 'edfs2-ls.html', {'queryset': files})
    # 考虑重定向去原来的页面，这样url会变

def analytics(request):
    return render(request, 'analytics.html')

def report(request):
    return render(request, 'report.html')
def remove(request):
    if request.method == 'GET':
        return render(request, 'remove-request.html')
    if request.method == 'POST':
        requestPath = request.POST.get('title')
        print(requestPath)
        result = edfs.remove(requestPath)
        print(result)
        filePath = requestPath.split('/')[:-1]
        filePath = '/'.join(filePath)
        print('file path: ', filePath)
        files = edfs.ls(filePath)
        # return render(request, 'edfs2-ls.html', {'queryset': files})
        edfs.currentPath = filePath
        print('remove: ' + edfs.currentPath)
        return redirect('/edfs2/')



