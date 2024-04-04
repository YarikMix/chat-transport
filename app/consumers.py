import json

from channels.generic.websocket import WebsocketConsumer
from asgiref.sync import async_to_sync

from app.utils import text_to_bits


class ChatConsumer(WebsocketConsumer):
    def connect(self):
        print("connect")

        self.room_group_name = "test"

        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name
        )

        self.accept()

        self.send(text_data=json.dumps({
            "type": "connection_established",
            "message": "You are now connected"
        }))

    def receive(self, text_data):
        text_data_json = json.loads(text_data)
        message = text_data_json["message"]

        print("receive")
        print(text_data)
        print(message)

        a = text_to_bits(message)
        print(a)
        print(len(a))
        f = ""
        for i in range(len(a)):
            if i % (130 * 8) == 0 and i != 0:
                print("Отправка сегментов")
                print(f)
                f = a[i]
            else:
                f += a[i]
        else:
            print("Отправка сегментов")
            print(f)

        print("END")

        async_to_sync(self.channel_layer.group_send)(
            self.room_group_name,
            {
                "type": "chat_message",
                "message": message
            }
        )

    def chat_message(self, event):
        message = event["message"]
        print("chat_message")
        self.send(text_data=json.dumps({
            "type": "chat",
            "message": message
        }))
