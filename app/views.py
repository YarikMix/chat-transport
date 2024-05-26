import threading
import time
from django.core.cache import cache

import requests
from rest_framework.decorators import api_view
from rest_framework.response import Response

from app.queue import sendKafka, getKafka

from app.utils import text_from_bits, text_to_bits


def send_to_ws(username, send_time, message, error=False):
    print("send_to_ws")
    resp = requests.post('http://localhost:4000/receive/', json={
        "user": username,
        "time": send_time,
        "message": message,
        "error": error
    })
    print(resp.status_code)


def pooling(username, send_time, message_id, segments_count):
    print("start pooling")

    prev = []

    while True:
        time.sleep(1)

        segments = cache.get(message_id)
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
                cache.delete(message_id)
            else:

                print("Success")

                print("segments: " + str(segments))

                try:
                    message = text_from_bits("".join(segments))
                    print("decoded: " + message)
                    send_to_ws(username, send_time, message)
                except:
                    send_to_ws(username, send_time, "", error=True)
                finally:
                    cache.delete(message_id)

            break


def assembling(message_id, segments_count, send_time, username):
    print("Сборка сегментов сообщения с id" + message_id)

    segments = getKafka(message_id)
    if segments_count == len(segments):
        for i, segment in enumerate(segments):
            print(f"Отправка сегмента №{i + 1} на канальный уровень")
            resp = requests.post('http://127.0.0.1:5000/Segments/Code/', json={
                "segment": segment,
                "total_segments": len(segments),
                "segment_number": i,
                "message_id": message_id,
                "send_time": send_time,
                "username": username
            })

            print(resp.status_code)

            time.sleep(1)


@api_view(["POST"])
def transfer(request):
    print("transfer")
    print("Ответка от канального уровня")
    print(request.data)

    message_id = request.data["Message_id"]
    segment = request.data["Segment"]
    segments_count = request.data["Total_segments"]
    segment_number = request.data["Segment_number"]
    username = request.data["Username"]
    send_time = request.data["Send_time"]

    if message_id in cache:
        cached_message = cache.get(message_id)
        cache.set(message_id, cached_message + [segment])
    else:
        cache.set(message_id, [segment])
        threading.Thread(target=pooling, args=[username, send_time, message_id, segments_count]).start()

    print("Редис: ")
    print(cache.get(message_id))

    return Response("OK")


@api_view(["POST"])
def send(request):
    print("receive")
    print(request.data)

    message = request.data["message"]
    message_id = request.data["time"]
    send_time = request.data["time"]
    username = request.data["user"]

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
        print(f"Отправка сегмента в кафку №{i + 1}")
        sendKafka(segment, message_id)

    threading.Thread(target=assembling, args=[message_id, len(segments), send_time, username]).start()

    return Response("OK")