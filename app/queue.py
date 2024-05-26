from json import dumps

from kafka import KafkaProducer, KafkaConsumer


def sendKafka(message, key="testnum"):
    print("sendKafka")
    print(message, key)
    my_producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    my_producer.send(key, value=message)


def getKafka(key="testnum"):
    print("getKafka")
    consumer = KafkaConsumer(key,
                             bootstrap_servers=['localhost:29092'],
                             group_id='test',
                             auto_offset_reset='earliest')

    res_str = []

    message = consumer.poll(0.5)

    for records in message.values():
        for msg in records:
            res_str.append(msg.value.decode("utf-8").strip('"'))

    return res_str


