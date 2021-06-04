from kafka import KafkaConsumer
consumer = KafkaConsumer('test_topic', bootstrap_servers='localhost:9092')
print("starting consumer...")
consumer_msgs = []
for line in consumer:
    line = line.decode('utf-8')
    consumer_msgs.append(line)
    if line[0] == '1':
        if 'eval' in line:
            consumer_msgs.append('[analyzed "True"]')
        else:
            consumer_msgs.append('[analyzed "False"]')
        write_row()
        consumer_msgs = []
    print(msg.value.decode('utf-8'))


