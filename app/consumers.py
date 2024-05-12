import json

import requests
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
        print("receive")
        data = json.loads(text_data)
        print(data)

        message = data["message"]

        message_id = data["time"] + data["user"] # TODO: генерить уникальным

        print("receive")
        print("message: " + message)
        print("message_id: " + message_id)

        a = text_to_bits(message)

        segments = []

        acc = ""
        for i in range(len(a)):
            if i % (130 * 8) == 0 and i != 0:
                segments.append(acc)
                acc = a[i]
            else:
                acc += a[i]
        else:
            segments.append(acc)

        print("END")

        for i, segment in enumerate(segments):
            print(f"Отправка сегмента №{i+1}")
            resp = requests.post('http://127.0.0.1:5000/Segments/Code/', json={
                "segment": segment,
                "total_segments": len(segments),
                "message_id": message_id,
                "send_time": data["time"],
                "username": data["user"]
            })

            print(resp.status_code)

    def chat_message(self, event):
        data = {
            "username": event["username"],
            "send_time":  event["send_time"],
            "message":  event["message"],
            "error": event["error"]
        }

        print("chat_message")
        print(data)

        self.send(text_data=json.dumps({
            "type": "chat",
            "data": data
        }))
