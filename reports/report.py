#
#This will trigger emails when certain thresholds are met
# The idea is lets say we automatically find out that the traffic is really bad
# in a specific part of towm then to inform authorities of the same.
#

from google.cloud import bigquery

# Import smtplib for the actual sending function

import smtplib

# Import the email modules we'll need

from email.mime.text import MIMEText
from smtplib import SMTPException
from email.header import Header
from email import encoders
from email.MIMEMultipart import MIMEMultipart
import os

#Define what's constitutes bad traffic 
bad_traffic=15.0

#Define global variables
dataset_id = 'chicago_historical_congestion_data'
table_id = 'live_traffic'
gmail_user="ganguly.sarthak@gmail.com"
gmail_password="licoysmyhjrjbavm"

#Get the bigquey client using which to send the query
client = bigquery.Client()

#Set various variables here
project_id='camel-154800' # GCP project ID
dataset_id = 'chicago_historical_congestion_data' #BQ dataset ID
table_id = 'live_traffic'
gmail_user="ganguly.sarthak@gmail.com"

gmail_password=os.environ['GMAIL_PASS']



dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
table = client.get_table(table_ref)  # API Request

server = smtplib.SMTP('smtp.gmail.com', 587)

server.ehlo()
server.starttls()
server.login(gmail_user, gmail_password)

# Get the data on what to send in the mail body and trigger the email

def send_email(data):
    
    data1="hello world"
    print("Trying to send emails")

   
    message = MIMEMultipart()
    message['From']='ganguly.sarthak@gmail.com'
    message['To']='ganguly.sarthak@gmail.com'
    message['Subject']=Header("Let me tell you about bad traffic","utf-8").encode()
    message.attach(MIMEText(data1))
    
    sender ="info@sarthak-python.com"
    receiver ="ganguly.sarthak@gmail.com"
    subject ="Let me tell you about bad traffic"
   
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (sender, ", ".join(receiver),subject , data)


    
    try:
       server = smtplib.SMTP('smtp.gmail.com', 587)
      
       server.ehlo()
       server.starttls()
       server.login(gmail_user, gmail_password)
       
       # send the message via the server set up earlier.
       
       server.sendmail(sender,receiver,message)
       print("We sent an email")    
       

    except SMTPException as e:
           print "Authentication failed"

    del message
    server.quit()

# View table properties
# Debugging steps

print(table.schema)
print(table.description)
print(table.num_rows)

query= (
        'SELECT CURRENT_SPEED, REGION, LAST_UPDATED FROM' + project_id.dataset_id.table_id + 'ORDER BY LAST_UPDATED DESC LIMIT 100'
       )

query_job = client.query(
            query)

# Check if current speed of traffic is below a threshold then traffic is bad and send an email to whoever wants them

for row in query_job:  # API request - fetches results
                    
                    current_speed=float(row['CURRENT_SPEED'])
                    print("Received the response")
                    if current_speed < bad_traffic:
                       data="Current traffic is horrible in "+ row['REGION'] + 'and current speed is '+ str(current_speed)+'. This was recorded at '+row['LAST_UPDATED'] +' CDT'
                       print ("printing the data received "+data)
                       send_email(data)
