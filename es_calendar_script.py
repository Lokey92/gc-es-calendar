esfrom __future__ import print_function
import datetime
import io, os, sys, config
import pickle
import os.path
import json
import difflib
import pytz
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from elasticsearch import Elasticsearch
from elasticsearch.client import MlClient


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
       
class GoogleAuth:
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    def calendar_connect():
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open('token.pickle', 'wb') as token:
                    pickle.dump(creds, token)

        service = build('calendar', 'v3', credentials=creds)
        return service


class GoogleAPI:
    # API class for Google, retrieves calendar events from calendar ID.
    def __init__(self, service):
        self.service = service
    
    def get_json_events(self):
        # Call the Calendar API
        gc_get_api_body = ''
        now = datetime.datetime.utcnow().isoformat() + 'Z' # 'Z' indicates UTC time
        print('Getting the upcoming 10 events')
        events_result = self.service.events().list(calendarId='primary', timeMin=now,
                                            maxResults=10, singleEvents=True,
                                            orderBy='startTime').execute()
        events = events_result.get('items', [])
    
        #Parses events into a JSON structured body to process into calendar events.
        if not events:
            print('No upcoming events found.')
        for event in events:
            start = event['start'].get('dateTime', event['start'].get('date'))
            end = event['end'].get('dateTime', event['end'].get('date'))
            gc_get_api_body += str('\t{"description": "' + event['summary'].strip() + '", "start_time": "' + start + '", "end_time": "' + end + '"}, \n')
        
        #Adds final touches to JSON body, then calls on calendar_es method to connect and pass along ml calendar.
        #gc_get_api_body = '{ "events": [' + gc_get_api_body[:-3] + ']}'
        gc_get_api_body = '[' + gc_get_api_body[:-3] + ']'
        print(gc_get_api_body)
        return json.loads(gc_get_api_body)


class ElasticAuth: 
    # When config is set to cloud, this establishes a cloud connection.
    def cloud(es_cloud_id, es_user, es_pass):
        es_auth = Elasticsearch(
        cloud_id= es_cloud_id,
        http_auth=(es_user, es_pass),
        )
        return es_auth
    # Place more connection types here (WIP)


class ElasticAPI:
    # API class for Elastic, parses and sends new events to ES instance.
    def __init__(self, es_auth, es_calendar_id):
        self.es_auth = es_auth
        self.es_calendar_id = es_calendar_id

    # Passes a POST command to add new calendar events to ES instance.
    def ml_put_calendar_events(self, es_put_api_body):
        # If no new events exist, nix it.
        if not es_put_api_body:
            print('No new events found.')

        # Otherwise adds new events processed from the calendar filter method.
        else:
            MlClient(self.es_auth).post_calendar_events(self.es_calendar_id, es_put_api_body, params=None, headers=None)	
            print('POST command sent, applying events to ' + self.es_calendar_id + ':\n' + es_put_api_body)
    
    # Retrieves calendar events that exist in ES.
    def ml_get_calendar_events(self):
        #Raw data is retrieved, then filtered down to the events JSON object.
        es_get_api_raw = MlClient(self.es_auth).get_calendar_events(self.es_calendar_id, params=None, headers=None)
        es_get_api_raw = json.dumps(es_get_api_raw["events"])

        #Body is formed using events data, for loop removes needless objects. 
        es_get_api_body = json.loads(es_get_api_raw)
        utc = pytz.timezone('UTC')
        for element in es_get_api_body:
            del element['calendar_id']
            del element['event_id']
            element['start_time'] = datetime.datetime.utcfromtimestamp(float(element['start_time'])/1000).astimezone(tz=None).isoformat()
            element['end_time'] = datetime.datetime.utcfromtimestamp(float(element['end_time'])/1000).astimezone(tz=None).isoformat()

        print(json.dumps(es_get_api_body))  
        return es_get_api_body

    # Compares the incoming calendar events with existing ones from ES to filter/remove duplicates.
    def ml_put_calendar_filter(self, es_get_api_body, gc_get_api_body):
        #Maps out json objects to be used for set/lists.
        put_compare = map(json.dumps, gc_get_api_body)
        get_compare = map(json.dumps, es_get_api_body)
        items_set = set()
        result = list()

        #Loads JSON objects retrieved from GET API into set.
        for get_element in get_compare:
            items_set.add(get_element)     

        #Compares incoming PUT data to existing GET data, only appends objects that dont match anything from GET.
        for put_element in put_compare:
            if not get_element in items_set:
                # add to results
                result.append(put_element)
        
        #Filtered data is dumped and processed into a API-friendly format for ingest.
        if not result:
            #When there are no new events, return nothing.
            es_put_api_body = None
        else:
            # Since lists add escaped characters to items, several string replacers are needed to remove them. Makes JSON API friendly for ES.
            es_put_api_body = '{ "events": ' + json.dumps(result).replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}").replace("[\"", "[", 1).replace("]", "]}", 1)
        return  es_put_api_body

def main():
    #Loads up configuration.
    cfg = config.Config('script_config.cfg')

    #Establishes connection to ES instance.
    es_auth = ElasticAuth.cloud(cfg['elastic.cloud_id'], cfg['elastic.username'], cfg['elastic.password'])
    es_api = ElasticAPI(es_auth, cfg['elastic.calendar_id'])

    #Establishes connection to GC instance. 
    service = GoogleAuth.calendar_connect()
    gc_api = GoogleAPI(service)

    #Retrieves GC and ES calendar events for comparison before ingest.
    gc_get_api_body = gc_api.get_json_events()
    es_get_api_body = es_api.ml_get_calendar_events()

    #Filters the GC/ES events so that only new events are added to the calendar.
    es_put_api_body = es_api.ml_put_calendar_filter(es_get_api_body, gc_get_api_body)

    #Posts new events to ES.
    es_api.ml_put_calendar_events(es_put_api_body)

if __name__ == '__main__':
    main()
    
    

