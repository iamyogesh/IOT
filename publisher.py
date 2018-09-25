import paho.mqtt.client as mqtt #import the client1
from kafka import KafkaConsumer



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
d = Deployment()
d.subscribe_topic()
d.pubish_message()
