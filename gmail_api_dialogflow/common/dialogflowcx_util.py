from google.cloud import dialogflowcx_v3beta1 as dialogflow
import uuid 

class DialogflowClient:
    
    def __init__(self, configuration): 
      
        self.__project = configuration['default']['GOOGLE_CLOUD_PROJECT']
        self.__location = configuration["dialogflow"]["LOCATION"]
        self.__agent_id = configuration["dialogflow"]["AGENT_ID"]
        self.__api_endpoint = configuration["dialogflow"]["API_ENDPOINT"]
        self.___connection = self.get_connection()
        

    
    def get_connection(self):
        try:
            client_options = {"api_endpoint": self.__api_endpoint}
            session_client = dialogflow.SessionsClient(client_options=client_options)
            # print('successfully got the Dialogflow connection')
            return session_client
        except Exception as e:
            print('failed at Dialogflow connection ',e)
            return False
    
    def send_data(self, data):        
        
        if self.___connection:
            try:
                # session_id = str(uuid.uuid4())
                session_id = data['threadId']
                session = self.___connection.session_path(self.__project, self.__location, self.__agent_id, session_id)
                text_input = dialogflow.TextInput(text=data['message'])
                query_input = dialogflow.QueryInput(text=text_input,language_code='en')
                response = self.___connection.detect_intent(
                    request={"session": session, "query_input": query_input}
                ) 
                response_messages = [
                " ".join(msg.text.text) for msg in response.query_result.response_messages
                ]
                # print('successfully sent data to dialogflow')              
                return ' '.join(response_messages)
            
            except Exception as e:
                print('failed at publishing data ', e)
