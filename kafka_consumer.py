from kafka import KafkaConsumer
import pydoop.hdfs as hdfs
consumer = KafkaConsumer('testTopic',bootstrap_servers=['localhost:9092'])
hdfs_path = 'hdfs://localhost:9000/TwitterData/data.json'


for message in consumer:
   values = message.value.decode('utf-8')
   with hdfs.open(hdfs_path, 'at') as f:
       print(message.value)
       f.write(f"{values}\n")