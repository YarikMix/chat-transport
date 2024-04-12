import json

import requests
from channels.generic.websocket import WebsocketConsumer
from asgiref.sync import async_to_sync

from app.utils import text_to_bits

from kafka import KafkaProducer, KafkaConsumer


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

    def getKafka(self):
        print("getKafka")
        consumer = KafkaConsumer('testnum',
                                 bootstrap_servers=['localhost:29092'],
                                 group_id='test',
                                 auto_offset_reset='earliest')
        for msg in consumer:
            res_str = msg.value.decode("utf-8")
            print("Text:", res_str)

    def sendKafka(self):
        print("sendKafka")
        my_producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        message = "1"
        while message != "0":
            message = input("Type your message: ")
            my_producer.send("testnum", value=message)
            self.getKafka()

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
                r = requests.post('http://127.0.0.1:5000/Segments/Code', json={
                    "Segment": f
                })
                print(r.status_code)
                print(r.text)
                self.sendKafka(r.text)
                f = a[i]
            else:
                f += a[i]
        else:
            print("Отправка сегментов")
            r = requests.post('http://127.0.0.1:5000/Segments/Code', json={
                "Segment": f
            })
            print(r.status_code)
            print(r.text)
        print("END")

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
