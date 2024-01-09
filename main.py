from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import json
import argparse
import sys
import os


parser=argparse.ArgumentParser(description='Fire Data')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
print(args)



#DATASET_ID="erm2-nwe9"
#APP_TOKEN="QB89RdFtZjg1EFj8Ias9zQn3i"
#ES_HOST="https://search-cis9760emilyure-dgarmwivqwdrwgeejj3tfwudfm.us-east-2.es.amazonaws.com"
#ES_USERNAME="cis9760user"
#ES_PASSWORD="CIS9760fall!"
#INDEX_NAME="fire"


DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]



if __name__=="__main__":
    
    try:
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    #We are specifying the columns and define what we want the data to be.
                    #However, it is not guaranteed that the data will come out clean. 
                    #We will might need to clean it in the next steps.
                    #If the data you're pushing to the Elasticsearch is not compatible with these definitions, 
                    #you'll either won't be able to push the data to Elasticsearch in the next steps 
                    #and get en error due to that or the columns will not be usable in Elasticsearch 
                    "properties": {
                        "incident_datetime": {"type": "date"},
                        "incident_response_seconds_qy": {"type": "float"},
                        "incident_borough": {"type": "keyword"},
                        "starfire_incident_id": {"type": "keyword"},
                        #"incident_zip": {"type": "float"}, #This should normally be considered for keyword 
                        #but I need a numeric field for the next steps. 
                    }
                },
            }
        )
        resp.raise_for_status()
        print(resp.json())
    except Exception as e:
        #print("Index already exists! Skipping!")
    
        client=Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
        total_number_of_rows = client.get(DATASET_ID,select='COUNT(*)', where="starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL")
        total_number_of_rows = int(total_number_of_rows[0]['COUNT'])
    
    if args.num_pages == None:
        args.num_pages = ceil(total_number_of_rows/arg.page_size)
        print("Total number of rows:", total_number_of_rows)
    
    
    for page in range(0,args.num_pages):
        client=Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
        rows=client.get(DATASET_ID, limit=args.page_size, where="starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL", offset=page*args.page_size)
      
        es_rows=[]
    
        
        for row in rows:
            try:
                # Convert
                es_row = {}
                es_row["incident_datetime"] = row["incident_datetime"]
                es_row["incident_response_seconds_qy"] = float(row["incident_response_seconds_qy"])
                es_row["incident_borough"] = row["incident_borough"]
                es_row["starfire_incident_id"] = row["starfire_incident_id"]

            #There might be still some bad data coming from the source
            #For instance, incident_zip might have N/A instead of numerical values.
            #In this case, the conversion will not work and the program will crash.
            #We do not want that. That's why we raise an exception here. 
            except Exception as e:
                print (f"Error!: {e}, skipping row: {row}")
                continue
            
            es_rows.append(es_row)
            #print(es_rows)
    
    
    
        
        bulk_upload_data = ""
        for line in es_rows:
            #print(f'Handling row {line["starfire_incident_id"]}')
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
        print (bulk_upload_data)
    
            
        try:
        # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
            # We upload es_row to Elasticsearch
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            print ('Done')
                
            # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}")
    
            