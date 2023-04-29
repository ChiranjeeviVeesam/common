from abc import ABC, abstractmethod
import os
from google.cloud import pubsub_v1

class AbstractBrokerClient(ABC):
    def __init__(self):
        pass 
    
    @abstractmethod
    def get_connection(self):
        raise NotImplementedError
    
    @abstractmethod 
    def send_data(self):
        raise NotImplementedError

class PubSubClient(AbstractBrokerClient):
    
    def __init__(self):
        pass 
    
    def get_connection(self):
        try:
            con = pubsub_v1.PublisherClient()
            print('successfully got the pubsub connection')
            return con
        except Exception as e:
            print('failed at pubsub connection ',e)
            return False
    
    def send_data(self, data):
        pub_con = self.get_connection()
        
        if pub_con:
            try:
                topic_path = pub_con.topic_path('peak-emitter-377300','watch-message')
                message_bytes = str(data).encode('utf-8')
                pub_con.publish(topic_path, message_bytes)
                print('successfully published the data')
            except Exception as e:
                print('failed at publishing data ', e)
    
    
        
