from extract_manager import ExtractManager 
from load_manager import LoadManager
import base64
import json 
import os 
from util import config_info
def main(event, context):
  try:
    
    configuration = config_info()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=configuration["default"]["GOOGLE_APPLICATION_CREDENTIALS"]
    os.environ["GOOGLE_CLOUD_PROJECT"]=configuration["default"]["GOOGLE_CLOUD_PROJECT"]
    
    data_str = base64.b64decode(event['data']).decode('utf-8')
    data_dict = json.loads(data_str)
    latest_history_id = data_dict['historyId']

    extract_manager_client = ExtractManager(configuration)
    load_manager_client = LoadManager(configuration)  

    previous_history_id = extract_manager_client.get_history_id()    
    load_manager_client.save_history_id(latest_history_id)
  
    if previous_history_id:
        print(f'started fetching data from gmail with history id {previous_history_id}')
        total_data =  extract_manager_client.get_gmail_data(previous_history_id)
        print(f'completed fetching data from gmail with history id {previous_history_id}')
        if total_data:
          load_manager_client.send_response_to_user_from_dialogflow(total_data) 
        else:
          print('No new data found')      
                            
    else:
        print('no history')
  except Exception as e:
    print('failed at main function', e)

    
    
