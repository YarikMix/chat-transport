import json

import requests
from channels.generic.websocket import WebsocketConsumer
from asgiref.sync import async_to_sync

from app.queue import sendKafka
from app.utils import text_to_bits, text_from_bits


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
        print("receive")
        data = json.loads(text_data)
        print(data)
        message = data["message"]

        print("receive")
        print(text_data)
        print(message)

        a = text_to_bits(message)
        print(a)
        print(len(a))

        f = ""
        for i in range(len(a)):
            if i % (130 * 8) == 0 and i != 0:
                print("Отправка сегмента")
                requests.post('http://127.0.0.1:5000/Segments/Code', json={
                    "time": data["time"],
                    "user": data["user"],
                    "Segment": f
                })
                f = a[i]
            else:
                f += a[i]
        else:
            print("Отправка сегмента")
            requests.post('http://127.0.0.1:5000/Segments/Code', json={
                "time": data["time"],
                "user": data["user"],
                "Segment": f
            })

        print("END")

    def chat_message(self, event):
        data = event["data"]
        print("chat_message")
        print(data)
        self.send(text_data=json.dumps({
            "type": "chat",
            "data": data
        }))

    def error_message(self, event):
        print("error_message")
        self.send(text_data=json.dumps({
            "type": "error",
            "data": "Ошибка"
        }))