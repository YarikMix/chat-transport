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
        text_data_json = json.loads(text_data)
        print(text_data_json)
        message = str(text_data_json)

        print("receive")
        print(text_data)
        print(message)

        a = text_to_bits(message)
        print(a)
        print(len(a))

        acc = ""

        f = ""
        for i in range(len(a)):
            if i % (130 * 8) == 0 and i != 0:
                print("Отправка сегментов")
                r = requests.post('http://127.0.0.1:5000/Segments/Code', json={
                    "Segment": f
                })
                print(r.status_code)
                data = json.loads(r.text)
                print(data["segment"])
                acc += data["segment"]
                # sendKafka(data["segment"])
                f = a[i]
            else:
                f += a[i]
        else:
            print("Отправка сегментов")
            r = requests.post('http://127.0.0.1:5000/Segments/Code', json={
                "Segment": f
            })
            print(json.loads(r.text))
            data = json.loads(r.text)
            # sendKafka(data["segment"])
            print(r.status_code)
            print(data["segment"])
            acc += data["segment"]

        print("END")


        try:
            print("Раскодированное сообщение")
            print(text_from_bits(acc).replace('\'', "\""))

            data = json.loads(text_from_bits(acc).replace('\'', "\""))
            async_to_sync(self.channel_layer.group_send)(
                self.room_group_name,
                {
                    "type": "chat_message",
                    "data": data
                }
            )
        except:
            print("Ошибка при парсинге JSON")
            async_to_sync(self.channel_layer.group_send)(
                self.room_group_name,
                {
                    "type": "error_message"
                }
            )

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