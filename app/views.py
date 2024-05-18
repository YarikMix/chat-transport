import threading
import time

import requests
from rest_framework.decorators import api_view
from rest_framework.response import Response

from app.queue import sendKafka, getKafka

from kafka import KafkaConsumer

from app.utils import text_from_bits, text_to_bits


def send_to_ws(username, send_time, message, error=False):
    resp = requests.post('http://localhost:4000/receive/', json={
        "user": username,
        "time": send_time,
        "message": message,
        "error": error
    })
    print(resp.status_code)


def pooling(username, send_time, message_id, segments_count):
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
                send_to_ws(username, send_time, "", error=True)
            else:

                print("Success")

                print("segments: " + str(segments))

                try:
                    message = text_from_bits("".join(segments))
                    print("decoded: " + message)
                    send_to_ws(username, send_time, message)
                except:
                    send_to_ws(username, send_time, "", error=True)

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
        a = threading.Thread(target=pooling, args=[username, send_time, message_id, segments_count])
        a.start()

    sendKafka(segment, message_id)

    return Response("OK")

@api_view(["POST"])
def receive(request):
    print("receive")
    print(request.data)

    message = request.data["message"]

    message_id = request.data["time"]

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
        print(f"Отправка сегмента №{i + 1}")
        resp = requests.post('http://127.0.0.1:5000/Segments/Code/', json={
            "segment": segment,
            "total_segments": len(segments),
            "message_id": message_id,
            "send_time": request.data["time"],
            "username": request.data["user"]
        })

        print(resp.status_code)
        print(resp.reason)

    return Response("OK")