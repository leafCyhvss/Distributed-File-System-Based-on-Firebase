from django.shortcuts import render

# Create your views here.
def helloworld(request):
    # 获取数据信息
    # edfs = EDFSURL()
    # filePaths = edfs.ls('/test1')
    # print(filePaths)
    return render(request, 'ls.html', {'queryset': ['/']})