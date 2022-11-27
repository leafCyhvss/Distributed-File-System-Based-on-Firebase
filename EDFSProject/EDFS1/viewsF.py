from django.shortcuts import render
from EDFS1 import edfs
# Create your views here.
def helloworld(request):
    # 获取数据信息
    # edfs = EDFSURL()
    # filePaths = edfs.ls('/test1')
    # print(filePaths)
    return render(request, 'edfs1-ls.html', {'queryset': ['/']})

def lsDisplay(request):
    if request.method == 'GET':
        root = ['/']
        return render(request, 'edfs1-lspost.html', {'queryset': root})
    if request.method == 'POST':
        # edfs = EDFSURL()
        requestPath = request.POST.get('title')
        requestPath = requestPath if requestPath else '/'
        filePaths = edfs.ls(requestPath)['data']
        print(filePaths)
        return render(request, 'edfs1-ls.html', {'queryset': filePaths})