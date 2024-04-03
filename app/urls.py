from django.urls import path

from .views import *

urlpatterns = [
    path('send/', sender),
    path('upload/', getter)
]