from json import dumps

from kafka import KafkaProducer, KafkaConsumer


def sendKafka(message, key="testnum"):
    print("sendKafka")
    my_producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    my_producer.send(key, value=message)
    getKafka()


def getKafka(key="testnum"):
    print("getKafka")
    consumer = KafkaConsumer(key,
                             bootstrap_servers=['localhost:29092'],
                             group_id='test',
                             auto_offset_reset='earliest')
    for msg in consumer:
        res_str = msg.value.decode("utf-8")
        print("Text:", res_str)
