import os
from confluent_kafka import Producer, Consumer, KafkaError

#Biblioteca para Python & Kafka

class kafkaAPI():

    @staticmethod
    def kafkaUtils_createProducerWithKerberos(self, topic_name, kafka_ip, group_id='group_consumer', kerberosKeyTabPath, kerberosUsername):
        conf = {
            'bootstrap.servers': kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        conf['security.protocol'] = 'SASL_PLAINTEXT'
        conf['sasl.mechanism'] = 'GSSAPI'
        conf['sasl.kerberos.service.name'] = 'kafka'
        conf['sasl.kerberos.keytab'] = os.environ[kerberosKeyTabPath]
        conf['sasl.kerberos.principal'] = os.environ[kerberosUsername]
        
        c = Consumer(conf)
        c.subscribe([topic_name])
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            val = msg.value().decode('utf-8')
            val = val.replace('"', '')
            self.onReceiveEvent([val], base_id="LIBRA")
        c.close()

    @staticmethod
    def kafkaUtils_createProducerWithLoginAndPass(self, topic_name, kafka_ip, group_id='group_consumer', kafkaUserName, kafkaPassword):
        
        conf = {
            'bootstrap.servers': kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        
        conf['security.protocol'] = 'SASL_PLAINTEXT',
        conf['sasl.mechanism'] = 'PLAIN'
        conf['sasl.username'] = os.environ[kafkaUserName]
        conf['sasl.password'] = os.environ[kafkaPassword]

    
        c = Consumer(conf)
        c.subscribe([topic_name])
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            val = msg.value().decode('utf-8')
            val = val.replace('"', '')
            self.onReceiveEvent([val], base_id="LIBRA")
        c.close()

    @staticmethod
    def kafkaUtils_createProducerWithoutLogin(self, topic_name, kafka_ip, group_id='group_consumer'):
        conf = {
                        'bootstrap.servers': kafka_ip,
                        'group.id': group_id,
                        'auto.offset.reset': 'earliest'
        }
    
        c = Consumer(conf)
        c.subscribe([topic_name])
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            val = msg.value().decode('utf-8')
            val = val.replace('"', '')
            self.onReceiveEvent([val], base_id="LIBRA")
        c.close()

