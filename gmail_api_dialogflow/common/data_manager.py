from abc import ABC, abstractmethod
from google.cloud import storage
import os 
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.cloud import secretmanager
from email.message import EmailMessage
import base64
import json
import uuid
import re 

class AbstractDataClient(ABC):
    def __init__(self):
        pass 
    
    @abstractmethod
    def get_connection(self):
        raise NotImplementedError
    
    @abstractmethod 
    def retrieve_data(self):
        raise NotImplementedError
    
    @abstractmethod 
    def send_data(self, data):
        raise NotImplementedError

class GoogleCloudStorageClient(AbstractDataClient):
    def __init__(self, configuration):
        self.__bucket_name = configuration["gcs"]['bucket_name']
        self.__blob_name = configuration["gcs"]['blob_name']
        self.__connection = self.get_connection()
    
    def get_connection(self):
        try:
            con = storage.Client()
            # print('google cloud storage connection is successful')
            return con
        except Exception as e:
            print('failed to connect Google Cloud Storage ',e)
            return False
       
    def retrieve_data(self):
        if self.__connection:
            try:
                bucket = self.__connection.bucket(self.__bucket_name)
                blob_obj = bucket.blob(self.__blob_name)
                
                history_id = blob_obj.download_as_text()
                print('history id retreived successfully')
                return history_id
            except Exception as e:
                print('failed to retrieve history id',e)
                return False
        return False 
    
    def send_data(self, history_id):
        if self.__connection:
            try:
                bucket = self.__connection.bucket(self.__bucket_name)
                blob_obj = bucket.blob(self.__blob_name)
                # history_id_str = str(json.loads(base64.b64decode(history_id).decode('utf-8')))
                blob_obj.upload_from_string(str(history_id)) 
                print('history id uploaded successfully')               
                return True
            except Exception as e:
                print('failed in uploading history id data ',e)
                return False
        return False 
        
        

class GmailClient(AbstractDataClient):
    
    def __init__(self, configuration):
        self.__secret_name = configuration["secret_manager"]["secret_name"]
        self.__project_id = configuration["default"]["GOOGLE_CLOUD_PROJECT"]
        self.__user_id = configuration['default']['USER_ID']
        self.__service = self.get_connection()
    
    def get_credentials(self, secret_name, project_id):

        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(name=name)
            payload = response.payload.data.decode('UTF-8')
            creds = Credentials.from_authorized_user_info(info=json.loads(payload))
            # print('fetched credentials successfully')
            return creds

        except Exception as e:
            print('failed at getting credentials', e)
            return False

    def get_connection(self, secret_name = None):
        
        # scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        try:
            if secret_name is None:
              credentials = self.get_credentials(self.__secret_name, self.__project_id)
            else:
              credentials = self.get_credentials(secret_name, self.__project_id)
            gmail_con = build('gmail', 'v1', credentials=credentials)
            # print('successfully connected to gmail api')
            return  gmail_con
        except Exception as e:
            print('failed to connect gmail api',e)
            return False
        
    def get_subject(self, data):
        try:
            headers = data['payload']['headers']
            for header in headers:
                if header['name']=='Subject':
                    return header['value']
            return ''
        except Exception as e:
            print('failed to get the subject',e)
            return False
        
    def get_from_email_address(self, data):
        try:
            headers = data['payload']['headers']
            for header in headers:
                if header['name']=='From':
                    return header['value']
            return ''
        except Exception as e:
            print('failed to get the from email address',e)
            return False
    def get_message_id(self, data):
        try:
            headers = data['payload']['headers']
            for header in headers:
                if header['name']=='Message-ID':
                    return header['value']
            return ''
        except Exception as e:
            print('failed to get the message id',e)
            return False 
    def get_to_email_address(self, data):
        try:
            headers = data['payload']['headers']
            for header in headers:
                if header['name']=='To':
                    return header['value']
            return ''
        except Exception as e:
            print('failed to get the to email address',e)
            return False 
            
    def get_message(self, data):
        try:
            message_data = data['snippet']
            # to maintain followup data by excluding the old data in reply mode 
            # match = re.search("wrote:",message_data)
            # if match:
            #   index = message_data[:match.start()].rfind('On')  
            #   message_data = message_data[:index] if index>0 else message_data
            return message_data
        except Exception as e:
            print('failed to get the message data',e)
            return False
    
    def retrieve_custom_data(self, unique_message_ids):
        output_list = []
        for id in unique_message_ids:
            try:
             data = self.__service.users().messages().get(userId=self.__user_id, id=id).execute()      
             message = self.get_message(data)
             from_email_addr = self.get_from_email_address(data)
             to_email_addr = self.get_to_email_address(data)
             subject = self.get_subject(data)
             #threadId maintains the context of mail thread
             threadId = data["threadId"]
             messageId = self.get_message_id(data)
             if subject and message and from_email_addr and to_email_addr and 'INBOX' in data['labelIds']:
                output = dict(zip(('subject','fromEmail','toEmail','message','threadId','messageId'),(subject,from_email_addr, to_email_addr, message, threadId, messageId)))     
                output_list.append(output)             
            except Exception as e:
                print('failed at extracting data',e)
        return output_list     
    def retrieve_data(self, history_id):        
        unique_messages = {}
        if self.__service:
            try:
                history_list = self.__service.users().history().list(userId=self.__user_id,startHistoryId=history_id,historyTypes='messageAdded', labelId='INBOX').execute()            
                if 'history' in history_list:
                    unique_message_ids = {message['id'] for history in history_list['history'] for message in history['messages']}
                    # print('successfully retrieved the data')
                    return self.retrieve_custom_data(unique_message_ids)
            except Exception as e:
                print('failed at retreiving message ids', e)

    def send_data(self, data):
        if self.__service:
            try:
                message = EmailMessage()
                message.set_content(data['response'])
                data = data['data']
                message['To'] = data['fromEmail']
                message['From'] =  data['toEmail']
                message['Subject'] = data['subject']
                message['In-Reply-To']=data['messageId']
                message['References']=data['messageId']

                
                

        # encoded message
                encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

                create_message = {
                    'threadId': data['threadId'],
                    'raw': encoded_message
                }
                
                send_message = (self.__service.users().messages().send
                        (userId=self.__user_id, body=create_message).execute())
                return send_message
                # print(F'Message Id: {send_message["id"]}')

            except Exception as error:
                print(F'An error occurred: {error}')
