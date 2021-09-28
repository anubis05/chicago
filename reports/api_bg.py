from google.cloud import bigquery

from flask import Flask, url_for
from flask import Response
from flask import request
import json

app = Flask(__name__)

client = bigquery.Client()
dataset_id = 'chicago_historical_congestion_data'
table_id = 'live_traffic'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
table = client.get_table(table_ref)  # API Request


#This is just hello world API call to check the status. If this returns welcome then 
# it means the API is up and running

@app.route('/')
def api_root():
    return 'Welcome'



# the query to receive regionId and send corresponding data from BQ`
# The API will send back current speed of vehicles, when was it last updated and some other metadata to the caller

@app.route('/regions', methods=['GET'])
def api_article():
    
    # View table properties
    print("Number of rows in table are " +str(table.num_rows))
    regionIdFromQuery = request.args.get('regionid')
   
    query= (
        'SELECT * FROM `camel-154800.chicago_historical_congestion_data.live_traffic` WHERE REGION_ID = @regionid ORDER BY LAST_UPDATED DESC LIMIT 1'
       )
     
    #Configure query params which is based on what I received from query
    query_params = [
          bigquery.ScalarQueryParameter('regionId', 'STRING', regionIdFromQuery),
    ]

    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    
    #Execute the remote query
    query_job = client.query(query,job_config=job_config)

    
    for row in query_job:  # API request - fetches results
                    
        current_speed=float(row['CURRENT_SPEED'])
        last_updated=str(row['LAST_UPDATED'])
        region_id=str(row['REGION_ID'])
        region=str(row['REGION'])
        description=str(str(row['DESCRIPTION']))
        
        print('Got valid responses from bigquery' + str(current_speed))

        #Create the json object which I want to send back
        data ={}
        data['CURRENT_SPEED'] = current_speed
        data['LAST_UPDATED'] = last_updated
        data['REGION_ID'] = region_id
        data['REGION'] = region
        data['DESCRIPTION'] = description
        
        json_data = json.dumps(data)
        resp = Response(json_data, status=200, mimetype='application/json')
        return resp


if __name__ == '__main__':
    app.run(host= '0.0.0.0')
