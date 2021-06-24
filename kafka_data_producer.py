import json
import time
from faker import Faker
from bson import json_util
from kafka import KafkaProducer

BOOTSTRAP_SERVER = "localhost:29092"
TOPIC_NAME = "data_pengguna"
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
fake = Faker('id_ID')

while True:
    fake_name = fake.name()
    fake_date = fake.date("%Y-%m-%d")
    fake_alamat = fake.address()

    my_dict = {"nama": fake_name, "tanggal_lahir": fake_date, "alamat": fake_alamat}
    producer.send(TOPIC_NAME, json.dumps(my_dict, default=json_util.default).encode("utf-8"))
    print(my_dict)
    time.sleep(5)