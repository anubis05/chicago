# Calls the City of Chicago APIs and gets the data
# This data is then published to a pubsub
# This code was origially executed on appengine but obviously can be executed on
# any other compute infra


#!/usr/bin/env python

#import webapp2
import requests
import json
import base64
from google.cloud import pubsub_v1
from collections import OrderedDict
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import time
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

PORT_NUMBER = 8080
url= 'http://data.cityofchicago.org/resource/8v9j-bter.json'
apigee_format_json_url= 'http://amer-demo25-test.apigee.net/format-json'
gcp_project= 'camel-154800'
pubsub_name='big_ant'

class myHandler(BaseHTTPRequestHandler):
    
    # Use the PubSub client to publish the data to Pub Sub

    def do_GET(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(gcp_project, pubsub_name)
        
        #Make the call to city Of Chicago API to get the data
        
        response = requests.get(url)
        #Check if the response is 200 which means the API executed successfully
        
        if (response.ok):
                            full_message = response.json()

                            # Loop through all the json objects in the array
                            i = 0
                            print('Total number of region data received is '+str(len(full_message)))
                            while i< len(full_message): 
                                print('Now working on message number #### '+str(i))
      		                message=(full_message[i])   
                                 
                                #r=self.format_payload(full_message[i])
                                print('calling format_payload function now')
      		                
                                ORDERED_KEYS = ['_direction', '_fromst', '_last_updt','_length','_lif_lat','_lit_lat','_lit_lon','_strheading','_tost','_traffic','segmentid','start_lon','street']
                                ordered_json = OrderedDict((k, message[k]) for k in ORDERED_KEYS)
                                text=json.dumps(ordered_json)
                                
                                print('The response is ready to be published. This is how it looks now:')
                                print(text)
      		                
      		                
                                #Get a client and publish the message to the topic

                                  
                                publisher.publish(topic_path, data=text) 
      		                i += 1    
                                self.send_response(200) # Setting HTTP status code to 200 indicating the API executed successfully
                                self.send_header('Content-type','text/html')
                                self.end_headers()
                                
                                # Send the html message
                                
                                self.wfile.write("we just posted objects to PubSub successfully. Total objects posted"+str(i))
                                
   		                        print("We posted the messages successfully to PubSub")
	else :

   	     response.raise_for_status()  

try:
    
#Create a web server and define the handler to manage the
	#incoming request
	server = HTTPServer(('', PORT_NUMBER), myHandler)
	print 'Started httpserver on port ' , PORT_NUMBER
	
	#Wait forever for incoming htto requests
	server.serve_forever()

except KeyboardInterrupt:
	print '^C received, shutting down the web server'
	server.socket.close()

