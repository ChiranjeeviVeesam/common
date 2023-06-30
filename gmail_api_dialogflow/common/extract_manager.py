from data_manager import GoogleCloudStorageClient, GmailClient

class ExtractManager:
    
    DATABASES = {
        "gcs": GoogleCloudStorageClient,
        "gmail": GmailClient
    }

    def __init__(self, configuration):
        self.__database_client = None
        self.__configuration = configuration
    
    def get_history_id(self):
        self.__database_client = self.DATABASES["gcs"](self.__configuration)
        return self.__database_client.retrieve_data()
    
    def get_gmail_data(self, history_id):
        self.__database_client = self.DATABASES["gmail"](self.__configuration)
        return self.__database_client.retrieve_data(history_id)
