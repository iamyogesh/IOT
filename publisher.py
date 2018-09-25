import paho.mqtt.client as mqtt #import the client1
from kafka import KafkaConsumer
from kafka import KafkaProducer



class Deployment(object):


    def pubish_message(self):
            """
            publish a message to mqtt broker.
            """

            rabbitmq_ip_address = '192.168.0.6'
            instance_name = 'instance_rabbitmq'
            topic_name = "house/bulb3"
            rabbitmq_port = "50402"


            broker_address=rabbitmq_ip_address
            print "connected to host", broker_address
            print "topic name", topic_name
            print("creating new instance")
            #client = mqtt.Client("P1") #create new instance
            client = mqtt.Client(instance_name)
            print("connecting to broker{}".format(broker_address))
            print "connected to the port {}" .format(rabbitmq_port)
            client.connect(broker_address,rabbitmq_port) #connect to broker
            client.loop_start() #start the loop
            print("Publishing message to topic {}".format(topic_name))
            i = 0
            while i < 5:
                    print "sending heartbeat {}".format(i)
                    client.publish(topic_name,"OFF_"+str(i))
                    time.sleep(2) # wait
                    i +=1
                    client.loop_stop() #stop the loop
            print "done"

    def subscribe_topic(self):
        """
        listen to topic and prints a message
        """
        consumer = KafkaConsumer('testing-topic',bootstrap_servers=['192.168.0.6:9092'])
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                  message.offset, message.key,
                                                  message.value))

    def pubish_message(self):
        # Asynchronous by default
        producer = KafkaProducer(bootstrap_servers=['192.168.0.6:9092'])
        future = producer.send('my-topic', b'raw_bytes')

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            log.exception()
            pass

        # Successful result returns assigned partition and offset
        print (record_metadata.topic)
        print (record_metadata.partition)
        print (record_metadata.offset)

        # produce keyed messages to enable hashed partitioning
        producer.send('my-topic', key=b'foo', value=b'bar')
        producer = KafkaProducer(value_serializer=msgpack.dumps)
        producer.send('msgpack-topic', {'key': 'value'})


    def publish_message_way2(self):

            # produce json messages
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
        producer.send('json-topic', {'key': 'value'})

        # produce asynchronously
        for _ in range(100):
            producer.send('my-topic', b'msg')

        def on_send_success(record_metadata):
            print(record_metadata.topic)
            print(record_metadata.partition)
            print(record_metadata.offset)

        def on_send_error(excp):
            log.error('I am an errback', exc_info=excp)
            # handle exception

        # produce asynchronously with callbacks
        producer.send('my-topic', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

        # block until all async messages are sent
        producer.flush()

        # configure multiple retries
        producer = KafkaProducer(retries=5)


d = Deployment()
d.subscribe_topic()
d.pubish_message()
d.publish_message_way2()