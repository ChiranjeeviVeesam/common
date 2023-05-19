from data_manager import GoogleCloudStorageClient, GmailClient
from dialogflow_util import DialogflowClient
from broker_manager import PubSubClient

class LoadManager:    
    DATABASES = {
        "gcs": GoogleCloudStorageClient,
        "gmail": GmailClient
    }
    BROKER = {
        "pubsub": PubSubClient
    }
    CONVERSATIONAL_AI = {
        "dialogflow":DialogflowClient
    }
    
    def __init__(self, configuration):
        self.__database_client = None
        self.__dialogflow_client = None
        self.__configuration = configuration
    
    def send_response_to_user_from_dialogflow(self, total_data):
        self.__dialogflow_client = self.CONVERSATIONAL_AI["dialogflow"](self.__configuration)
        self.__database_client = self.DATABASES["gmail"](self.__configuration)
        if total_data:
          for data in total_data:
            response = self.__dialogflow_client.send_data(data)
            if response:
                self.__database_client.send_data({
                    'fromEmail':data['toEmail'],
                    'toEmail': data['fromEmail'],
                    'subject': data['subject'],
                    'response':response
                })
                # pass
            else:
                print('no response from dialogflow')
    
    
    def save_history_id(self, history_id):
        self.__database_client = self.DATABASES["gcs"](self.__configuration)
        return self.__database_client.send_data(history_id) 
    