"""EDFSProject URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

from EDFS1 import viewsF
from EDFS2 import viewsS

urlpatterns = [
    # main view
    path('', viewsF.helloworld),

    # EDFS1
    path('edfs1/', viewsF.edfs1Main),
    # cmds
    path('edfs1/ls-request', viewsF.lsDisplay),

    # search and analytics

    # EDFS2
    path('edfs2/', viewsS.helloworld),
    # cmds
    path('edfs2/ls-request', viewsS.lsDisplay),
    path('edfs2/part-request', viewsS.showPartition),
    path('edfs2/mkdir', viewsS.mkdir),
    path('edfs2/upload', viewsS.put),
    path('edfs2/remove', viewsS.remove),
    path('edfs2/cat', viewsS.catDisplay),
    path('edfs2/read-part', viewsS.readPart),

    # search and analytics
    path('edfs2/analytics.html', viewsS.analytics),
    path('edfs2/report.html', viewsS.report)
]
