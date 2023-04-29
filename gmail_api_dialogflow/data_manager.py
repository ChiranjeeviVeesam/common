from abc import ABC, abstractmethod
from google.cloud import storage
import os 
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.cloud import secretmanager
import base64
import json

class AbstractDataClient(ABC):
    def __init__(self):
        pass 
    
    @abstractmethod
    def get_connection(self):
        raise NotImplementedError
    
    @abstractmethod 
    def retrieve_data(self):
        raise NotImplementedError

class GoogleCloudStorageClient(AbstractDataClient):
    def __init__(self):
        pass
    
    def get_connection(self):
        try:
            con = storage.Client()
            print('google cloud storage connection is successful')
            return con
        except Exception as e:
            print('failed to connect Google Cloud Storage',e)
            return False
       
    def retrieve_data(self):
        gcs_con = self.get_connection()
        if gcs_con:
            try:
                bucket = gcs_con.bucket('tmp_poc')
                blob_obj = bucket.blob('historyid.txt')
                
                history_id = blob_obj.download_as_text()
                print('history id retreived successfully')
                return history_id
            except Exception as e:
                print('failed to retrieve history id',e)
                return False
        return False 
    
    def save_data(self, history_id):
        gcs_con = self.get_connection()
        if gcs_con:
            try:
                bucket = gcs_con.bucket('tmp_poc')
                blob_obj = bucket.blob('historyid.txt')
                # history_id_str = str(json.loads(base64.b64decode(history_id).decode('utf-8')))
                blob_obj.upload_from_string(str(history_id)) 
                print('history id uploaded successfully')               
                return True
            except Exception as e:
                print('failed in uploading history id data ',e)
                return False
        return False 
        
        

class GmailClient(AbstractDataClient):
    
    def __init__(self):
        self.__service = self.get_connection()
    
    def get_credentials(self, secret_name, project_id):

        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(name=name)
            payload = response.payload.data.decode('UTF-8')
            creds = Credentials.from_authorized_user_info(info=json.loads(payload))
            print('fetched credentials successfully')
            return creds

        except Exception as e:
            print('failed at getting credentials', e)
            return False

    def get_connection(self):
        
        # scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        try:
            secret_name = 'access_token'
            project_id = 'peak-emitter-377300'
            credentials = self.get_credentials(secret_name, project_id)
            gmail_con = build('gmail', 'v1', credentials=credentials)
            print('successfully connected to gmail api')
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
            print('failed to get the email address',e)
            return False 
        
    def get_message(self, data):
        try:
            return data['snippet']
        except Exception as e:
            print('failed to get the message data',e)
            return False
    
    def extract_message_data(self, unique_message_ids):
        for id in unique_message_ids:
            try:
             data = self.__service.users().messages().get(userId='me', id=id).execute() 
                    
             message = self.get_message(data)
             from_email_addr = self.get_from_email_address(data)
             subject = self.get_subject(data)
             if subject and message and from_email_addr:
                output = dict(zip(('subject','from email','message'),(subject,from_email_addr,message)))     
                yield output             
            except Exception as e:
                print('failed at extracting data',e)
             
    def retrieve_data(self, history_id):        
        unique_messages = {}
        if self.__service:
            try:
                history_list = self.__service.users().history().list(userId="me",startHistoryId=history_id,historyTypes='messageAdded', labelId='INBOX').execute()            
                if 'history' in history_list:
                    unique_message_ids = {message['id'] for history in history_list['history'] for message in history['messages']}
                    print('successfully retreived the data')
                    return self.extract_message_data(unique_message_ids)
            except Exception as e:
                print('failed at retreiving message ids', e)

        
        
    
