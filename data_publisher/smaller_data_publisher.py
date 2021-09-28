# There are two data feeds, one which produces relatively small amount of data and triggered every minute or so via cron job
# This code handles that file
# There is another feed which has much larger data volume. That feed produces more details about each datapoints. The larger_data_publisher
# contains the code to receive and parse that data and push it to a pubsub. The Larger dataset is received every 10 mins or so via another
#cron job


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

PORT_NUMBER = 8081
url= 'http://amer-demo25-test.apigee.net/chicago'
apigee_format_json_url= 'http://amer-demo25-test.apigee.net/format-json'
gcp_project= 'camel-154800'
pubsub_name='ant3'

class myHandler(BaseHTTPRequestHandler):

    def format_payload(self,message):
       try:
           r = requests.post(apigee_format_json_url, json=message)  
           print('Got response back from Apigee format-json method')
           return r
       except requests.ConnectionError as e:
              print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
              print(str(e))            
       except requests.Timeout as e:
              print("OOPS!! Timeout Error")
              print(str(e))
       except requests.RequestException as e:
              print("OOPS!! General Error")
              print(str(e))

    def do_GET(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(gcp_project, pubsub_name)
        
        #Make the call to city Of Chicago to get the data
        
        response = requests.get(url)
        
        #Check if the response is 200
        
        if (response.ok):
                            full_message = response.json()

                            # Loop through all the json objects in the array
                            i = 0
                            print('Total number of region data received is '+str(len(full_message)))
                            while i< len(full_message): 
                                print('Now working on message number #### '+str(i))
      		                message=str(full_message[i])   
                                 
                                r=self.format_payload(full_message[i])
                                print('calling format_payload function now')
      		                
                                text = r.content.decode(requests.utils.guess_json_utf(r.content)).encode('utf-8')
                                print('The response is ready to be published. This is how it looks now:')
                                print(text)
      		                
      		                
                                   #Get a client and publish the message to the topic

                                   
                                publisher.publish(topic_path, data=text) 
      		                i += 1    
                                self.send_response(200)
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
	
	#Wait forever for incoming http requests
	server.serve_forever()

except KeyboardInterrupt:
	print '^C received, shutting down the web server'
	server.socket.close()

