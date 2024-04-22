from rest_framework.decorators import api_view
from rest_framework.response import Response
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync



@api_view(["POST"])
def transfer(request):
    # a = text_to_bits(str(request.data))
    # f = ""
    # for i in range(len(a)):
    #     if i % (130 * 8) == 0 and i != 0:
    #         r = requests.post('http://127.0.0.1:8000/receive/', json=f)
    #         print(r.text)
    #         f = a[i]
    #     else:
    #         f += a[i]
    # else:
    #     r = requests.post('http://127.0.0.1:8000/receive/', json=f)
    #     print(r.text)
    # print("END")

    print("transfer")
    print(request.status)
    print(request.body)

    message = " to deploy"
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        'test',
        {'type': 'chat_message', 'data': message}
    )

    return Response("OK")

