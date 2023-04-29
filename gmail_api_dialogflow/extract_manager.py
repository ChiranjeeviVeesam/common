from data_manager import GoogleCloudStorageClient, GmailClient

class ExtractManager:
    
    DATABASES = {
        "gcs": GoogleCloudStorageClient,
        "gmail": GmailClient
    }

    def __init__(self):
        self.__database_client = None
    
    def get_history_id(self):
        self.__database_client = self.DATABASES["gcs"]()
        return self.__database_client.retrieve_data()
    
    def get_gmail_data(self, history_id):
        self.__database_client = self.DATABASES["gmail"]()
        return self.__database_client.retrieve_data(history_id) 
    


        
        
