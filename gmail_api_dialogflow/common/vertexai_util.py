import vertexai
from vertexai.language_models import TextGenerationModel

class VertexAIClient:
    
    def __init__(self, configuration=None): 
        self.__project = configuration['default']['GOOGLE_CLOUD_PROJECT']
        self.__location = configuration["default"]["location"]
        self.__model_name = configuration["vertex_ai"]["model_name"]
        self.__parameters =  {
        "temperature": 0.2,
        "max_output_tokens": 256,
        "top_p": 0.8,
        "top_k": 40
         }
        self.___connection = self.get_connection()

    def get_connection(self):
        try:
            vertexai.init(project=self.__project, location="us-central1")
            tgm_client = TextGenerationModel.from_pretrained(self.__model_name)
            print('successfully got the VertexAI connection')
            return tgm_client
        except Exception as e:
            print('failed at VertexAI connection ',e)
            return False
    
    def send_data(self, data):        
        
        if self.___connection:
            try:
                email_message = data['subject'] + "\n" + data['message']
                response = self.___connection.predict(email_message, **self.__parameters)
                print('sent data to Vertex AI')               
                return response.text
            
            except Exception as e:
                print('failed at publishing data to Vertex AI ', e)
