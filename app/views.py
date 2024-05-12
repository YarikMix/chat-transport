import threading
import time

from rest_framework.decorators import api_view
from rest_framework.response import Response
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

from app.queue import sendKafka, getKafka

from kafka import KafkaConsumer

from app.utils import text_from_bits


def send_to_websocket(username, send_time, message, error=False):
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        'test',
        {
            'type': 'chat_message',
            "username": username,
            "send_time": send_time,
            "message": message,
            "error": error
        }
    )


def test(username, send_time, message_id, segments_count):
    print("ruuuun")

    prev = []

    while True:
        time.sleep(1)

        segments = getKafka(message_id)
        if len(prev) != len(segments):
            print("segments")
            print(segments)
            prev = segments

        else:
            print(message_id + " :stoped")
            print("segments: " + str(len(segments)))

            if len(segments) != segments_count:
                print("Error!")
                send_to_websocket(username, send_time, "", error=True)
            else:

                # TODO: обработать сегменты

                print("Success")

                print("segments: " + str(segments))

                print("decoded: " + text_from_bits("".join(segments)))

                try:
                    message = text_from_bits("".join(segments))
                    send_to_websocket(username, send_time, message)
                except:
                    send_to_websocket(username, send_time, "", error=True)

            break


@api_view(["POST"])
def transfer(request):
    print("transfer")
    print(request.data)

    message_id = request.data["Message_id"]
    segment = request.data["Segment"]
    segments_count = request.data["Total_segments"]
    segment_number = request.data["Segment_number"]
    username = request.data["Username"]
    send_time = request.data["Send_time"]

    consumer = KafkaConsumer(bootstrap_servers=['localhost:29092'],
                             group_id='test',
                             auto_offset_reset='earliest')

    if message_id not in consumer.topics():
        print("Start thread: " + message_id)
        a = threading.Thread(target=test, args=[username, send_time, message_id, segments_count])
        a.start()

    sendKafka(segment, message_id)

    return Response("OK")
