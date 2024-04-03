from rest_framework.decorators import api_view
from rest_framework.response import Response
from .utils import text_to_bits

import requests


@api_view(["POST"])
def sender(request):
    a= text_to_bits(str(request.data))
    f=""
    for i in range(len(a)):
        if i%(130*8)==0 and i!=0:
            r = requests.post('http://127.0.0.1:8020/code/',json=f)
            print(r.text)
            f=a[i]
        else:
            f+=a[i]
    else:
        r = requests.post('http://127.0.0.1:8020/code/',json=f)
        print(r.text)
    print("END")
    return Response("OK")


@api_view(["POST"])
def getter(request):
    a= text_to_bits(str(request.data))
    f=""
    for i in range(len(a)):
        if i%(130*8)==0 and i!=0:
            r = requests.post('http://127.0.0.1:8000/receive/',json=f)
            print(r.text)
            f=a[i]
        else:
            f+=a[i]
    else:
        r = requests.post('http://127.0.0.1:8000/receive/',json=f)
        print(r.text)
    print("END")
    return Response("OK")

