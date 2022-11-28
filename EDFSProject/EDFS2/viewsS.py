from django.shortcuts import render, redirect
from EDFS2.EDFS2 import EDFSURL
from MapReduce.mapreduce import MapReducer
import pandas as pd
from django.urls import reverse

edfs = EDFSURL()


# Create your views here.
def helloworld(request):
    if request.method == 'GET':
        root = edfs.ls('/')['data']
        return render(request, 'edfs2-ls.html', {'msg': 'All path start with /', 'path': 'root (/)', 'queryset': root})
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        msg = edfs.ls(requestPath)['success']
        filePaths = edfs.ls(requestPath)['data']
        edfs.currentPath = requestPath
        print(filePaths)
        return render(request, 'edfs2-ls.html', {'msg': msg, 'path': edfs.currentPath, 'queryset': filePaths})


def lsDisplay(request):
    if request.method == 'GET':
        root = ['/']
        return render(request, 'edfs2-ls-request.html', {'queryset': root})
    if request.method == 'POST':
        # edfs = EDFSURL()x
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        msg = edfs.ls(requestPath)['success']
        filePaths = edfs.ls(requestPath)['data']
        edfs.currentPath = requestPath
        print(filePaths)
        return render(request, 'edfs2-ls.html', {'msg': msg, 'path': edfs.currentPath, 'queryset': filePaths})


def catDisplay(request):
    if request.method == 'GET':
        return render(request, 'edfs2-cat-request.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        # requestPath = requestPath if requestPath else '/'
        ans = edfs.cat(requestPath)
        msg = ans['success']
        if msg[0] == 'Cat Success':
            result = ans['data']
            # print(filePaths)
            print(result)
            # pd.set_option('colheader_justify', 'center')
            return render(request, 'edfs2-cat-result.html', \
                          {'msg': msg[0], 'table': result.to_html(classes="table table-bordered table-hover")})
        else:
            return render(request, 'edfs2-cat-result.html', \
                          {'msg': msg[0], 'table': 'Incorrect input'})


def showPartition(request):
    if request.method == 'GET':
        return render(request, 'edfs2-locpart-request.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'  # 判断空
        msg = edfs.getPartitionLocations(requestPath)['success'][0]
        filePaths = edfs.getPartitionLocations(requestPath)['data']
        print(filePaths)
        return render(request, 'edfs2-locpart-result.html', {'msg': msg, 'queryset': filePaths})


def mkdir(request):
    if request.method == 'GET':
        return render(request, 'edfs2-mkdir-reuqest.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        result = edfs.mkdir(requestPath)
        print(result)
        filePaths = edfs.ls(requestPath)['data']
        return render(request, 'edfs2-ls.html', {'msg': result[0], 'path': requestPath, 'queryset': filePaths})


def put(request):
    if request.method == 'GET':
        return render(request, 'edfs2-put-request.html')
    if request.method == 'POST':
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
        result = edfs.put('./localData/' + fileObject.name, filePath, pnumber)
        print(result)
        files = edfs.ls(filePath)['data']
        return render(request, 'edfs2-ls.html', {'msg': result[0], 'path': filePath, 'queryset': files})
    # 考虑重定向去原来的页面，这样url会变


def remove(request):
    if request.method == 'GET':
        return render(request, 'edfs2-remove-request.html')
    if request.method == 'POST':
        requestPath = request.POST.get('title')
        print(requestPath)
        requestPath = requestPath if requestPath else '/'
        print('re:', requestPath)
        if requestPath == '/':
            files = edfs.ls('/')['data']
            return render(request, 'edfs2-ls.html', \
                          {'msg': 'Remove ERROR: Should not remove root (/)', \
                           'path': 'root (/)', \
                           'queryset': files})
        result = edfs.remove(requestPath)
        print(result)
        filePath = requestPath.split('/')[:-1]
        filePath = '/'.join(filePath)
        print('file path: ', filePath)
        files = edfs.ls(filePath)['data']
        return render(request, 'edfs2-ls.html', {'msg': result[0], 'path': filePath, 'queryset': files})


def readPart(request):
    if request.method == 'GET':
        return render(request, 'edfs2-readpart-request.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('filepath')
        # requestPath = requestPath if requestPath else '/'
        pnumber = request.POST.get('pnumber')

        ans = edfs.readPartition(requestPath, int(pnumber))
        msg = ans['success']
        result = ans['data']
        if msg[0] == 'Read Partition Success':
            # pd.set_option('colheader_justify', 'center')
            return render(request, 'edfs2-readpart-result.html', \
                          {'msg': msg[0], 'table': result.to_html(classes="table table-bordered table-hover")})
        else:
            return render(request, 'edfs2-cat-result.html', \
                          {'msg': msg[0], 'table': 'Incorrect input'})

def search(request):
    if request.method == 'GET':
        return render(request, 'search-request.html')
    if request.method == 'POST':
        # edfs = EDFSURL()
        filePath1 = request.POST.get('filepath')
        # filePath2 = request.POST.get('filepath2')
        # price = int(request.POST.get('price'))
        trans = request.POST.get('trans')
        trans2 = request.POST.get('trans2')
        print(filePath1)
        # print(filePath2)
        # print(price)
        print('Transmission', trans, '???', trans2)
        return render(request, 'search-result.html')

def analytics(request):
    return render(request, 'analytics.html')


def report(request):
    return render(request, 'report.html')
