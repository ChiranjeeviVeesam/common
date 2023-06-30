from data_manager import GoogleCloudStorageClient, GmailClient
from vertexai_util import VertexAIClient

class LoadManager:    
    DATABASES = {
        "gcs": GoogleCloudStorageClient,
        "gmail": GmailClient
    }
    CONVERSATIONAL_AI = {
        "vertexai":VertexAIClient
    }
    
    def __init__(self, configuration):
        self.__database_client = None
        self.__conversational_client = None
        self.__configuration = configuration
    
    def send_response_to_user_from_dialogflow(self, total_data):
        self.__conversational_client = self.CONVERSATIONAL_AI["vertexai"](self.__configuration)
        self.__database_client = self.DATABASES["gmail"](self.__configuration)
        for data in total_data:
          print('started sending data to conversational client')
          response = self.__conversational_client.send_data(data)
          print('completed sending data to converational client')
          if response:
              print('started sending data to the user')
              send_message = self.__database_client.send_data({
                  'data':data,
                  'response':response
              })
              print(f'completed sending data to user with message id: {send_message["id"]}')
              # pass
          else:
              print('no response from dialogflow')
    
    
    def save_history_id(self, history_id):
        self.__database_client = self.DATABASES["gcs"](self.__configuration)
        return self.__database_client.send_data(history_id)
