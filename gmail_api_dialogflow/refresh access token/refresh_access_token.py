from google.oauth2.credentials import Credentials 
import os 
from google.auth.transport.requests import Request 
from google.cloud import secretmanager
import json 

def access_token_check(event, context):
    
    updated_creds = None
    creds = None
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='secretmanagerkey.json'
    os.environ["GOOGLE_CLOUD_PROJECT"]='peak-emitter-377300'
    
 
    creds = get_credentials('access_token','peak-emitter-377300')
    if creds:  
        updated_creds = refresh_access_token(creds)
        
    if updated_creds:
        upload_credentials('access_token','peak-emitter-377300',updated_creds)


def get_credentials(secret_name, project_id):

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

def upload_credentials(secret_name, project_id, creds):

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}"
        response = client.access_secret_version(name=f"{name}/versions/latest")
        client.add_secret_version(
        parent=name,
        payload={"data": creds.to_json().encode("UTF-8")})        
        client.disable_secret_version(request={"name":response.name})
        print('updated credentials successfully')

    except Exception as e:
        print('failed at uploading credentials',e)
        return False
    
def refresh_access_token(creds):

    try:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            print('Refreshed token successfully')
            return creds 
            
    except Exception as e:
        print('failed at refreshing access token',e)
        return False

        
          
