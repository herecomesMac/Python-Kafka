import os
from confluent_kafka import Producer, Consumer, KafkaError

#Biblioteca para Python & Kafka

class kafkaAPI():

    @staticmethod
    def createProducerWithKerberos(kafka_ip, group_id, autoOffsetReset, securityProtocol, saslMechanism, saslKerberosServiceName, kerberosKeyTabPath, kerberosUsername):
        
        conf = {
            'bootstrap.servers': kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': autoOffsetReset
        }
        conf['security.protocol'] = securityProtocol
        conf['sasl.mechanism'] = saslMechanism
        conf['sasl.kerberos.service.name'] = saslKerberosServiceName
        conf['sasl.kerberos.keytab'] = os.environ[kerberosKeyTabPath]
        conf['sasl.kerberos.principal'] = os.environ[kerberosUsername]
        
        c = Consumer(conf)
        return c
        

    @staticmethod
    def createProducerWithLoginAndPass(kafka_ip, group_id, autoOffsetReset, securityProtocol, saslMechanism, kafkaUserName, kafkaPassword):
        
        conf = {
            'bootstrap.servers': kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': autoOffsetReset
        }
        
        conf['security.protocol'] = securityProtocol,
        conf['sasl.mechanism'] = saslMechanism
        conf['sasl.username'] = os.environ[kafkaUserName]
        conf['sasl.password'] = os.environ[kafkaPassword]

    
        c = Consumer(conf)
        return c

    @staticmethod
    def createProducerWithoutLogin(kafka_ip, group_id, autoOffsetReset):
        conf = {
            'bootstrap.servers': kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
    
        c = Consumer(conf)
        return c

