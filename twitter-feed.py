import asyncio
import datetime
import random
import websockets
from kafka import KafkaConsumer
import json
import os


kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS","localhost:9092").split(",")
kafka_listen_topic = os.environ.get('KAFKA_LISTEN_TOPIC', 'lews-twitter').split(",")
websocket_port = os.environ.get('WEBSOCKET_SERVER_PORT', 5678)
print("Environment variables:")
print(f"KAFKA_BOOTSTRAP_SERVERS = {kafka_servers}")
print(f"KAFKA_LISTEN_TOPIC = {kafka_listen_topic}")
print(f"WEBSOCKET_SERVER_PORT = {websocket_port}")


async def time(websocket, path):
    consumer = KafkaConsumer(bootstrap_servers=kafka_servers,value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(kafka_listen_topic)
    for message in consumer:
        record = json.loads(message.value)
        if "raw_data" in record :
            user = record["raw_data"]["user"]["screen_name"]
            timestamp_ms = record["raw_data"]["timestamp_ms"]
            profile_image_url_https = record["raw_data"]["user"]["profile_image_url_https"]
            if record["raw_data"]["truncated"]:
                text = record["raw_data"]["extended_tweet"]["full_text"]
                print("Truncated:",record["raw_data"]["text"])
                print("Extended:",text)
            else:
                text = record["raw_data"]["text"]
                print("Small:",text) 
        else:
            user = record["user"]["screen_name"]
            timestamp_ms = record["timestamp_ms"]
            profile_image_url_https = record["user"]["profile_image_url_https"]
            if record["truncated"]:
                text = record["extended_tweet"]["full_text"]
                print("Truncated:",record["text"])
                print("Extended:",text)
            else:
                text = record["text"]
                print("Small:",text) 

        payload_dict = {}
        payload_dict["user"] = user
        payload_dict["text"] = text
        payload_dict["profile_image_url_https"] = profile_image_url_https
        payload_dict["timestamp_ms"] = timestamp_ms

        payload_json = json.dumps(payload_dict)


        await websocket.send(payload_json)

print("Starting websocket server on port ",websocket_port)

start_server = websockets.serve(time, "localhost", websocket_port)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
