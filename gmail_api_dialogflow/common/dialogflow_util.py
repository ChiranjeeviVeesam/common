from google.cloud import dialogflow
import uuid 

class DialogflowClient:
    
    def __init__(self, configuration):
        self.___connection = self.get_connection() 
        self.__project = configuration['default']['GOOGLE_CLOUD_PROJECT']
        
    
    def get_connection(self):
        try:
            session_client = dialogflow.SessionsClient()
            print('successfully got the Dialogflow connection')
            return session_client
        except Exception as e:
            print('failed at Dialogflow connection ',e)
            return False
    
    def send_data(self, data):        
        
        if self.___connection:
            try:
                # session_id = str(uuid.uuid4())
                session_id = data['fromEmail']
                session = self.___connection.session_path(self.__project, session_id)
                text_input = dialogflow.TextInput(text=data['message'],language_code='en')
                query_input = dialogflow.QueryInput(text=text_input)
                response = self.___connection.detect_intent(
                    request={"session": session, "query_input": query_input}
                )
                print('sent data to dialogflow')               
                return response.query_result.fulfillment_text
            
            except Exception as e:
                print('failed at publishing data ', e)
