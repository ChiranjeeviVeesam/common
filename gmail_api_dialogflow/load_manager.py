from data_manager import GoogleCloudStorageClient
from broker_manager import PubSubClient
class LoadManager:    
    DATABASES = {
        "gcs": GoogleCloudStorageClient
    }
    BROKER = {
        "pubsub": PubSubClient
    }
    
    def __init__(self):
        self.__database_client = None
    
    def publish_data(self, total_data):
        self.__database_client = self.BROKER["pubsub"]()
        if total_data:
          for data in total_data:
            self.__database_client.send_data(data)
    
    def save_history_id(self, history_id):
        self.__database_client = self.DATABASES["gcs"]()
        return self.__database_client.save_data(history_id) 
    
