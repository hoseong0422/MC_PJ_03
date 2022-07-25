from kafka import KafkaConsumer
import json
import pymongo

USER = "admin2"
PWD = "1111"
HOST = "ec2-54-248-99-240.ap-northeast-1.compute.amazonaws.com"
PORT = "27017"
client = pymongo.MongoClient(f"mongodb://{USER}:{PWD}@{HOST}:{PORT}")
db = client['test']

BROKERS = ["localhost:9092"]
TOPIC_NAME = "metacritic_info_topic"

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = BROKERS)


print("Wait...")
while True:
    for message in consumer:
        doc = json.loads(message.value.decode())
        db.meta_test.insert_one(doc)
        print(doc)

