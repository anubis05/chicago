import webapp2
import requests

import socket

socket.setdefaulttimeout(240)

#class MainPage(webapp2.RequestHandler):
 #   def get(self):
  #      self.response.headers['Content-Type'] = 'text/plain'
  #      self.response.write('PONG')


class MainPage(webapp2.RequestHandler):
    def get(self):
        url="http://35.237.63.142:8080/"
        response=requests.get(url)
        if (response.ok):
           self.response.headers['Content-Type'] = 'text/plain'
           self.response.write('successfully called the large dataset')
        else:
  # If response code is not ok (200), print the resulting http error code with description
           response.raise_for_status()   


class Smaller_dataset(webapp2.RequestHandler):
    def get(self):
        url="http://35.237.63.142:8081/"
        response=requests.get(url)
        if (response.ok):
           self.response.write('Successfully called the small dataset')


        else:
           response.raise_for_status()

app = webapp2.WSGIApplication([
    ('/smalldataset', Smaller_dataset),
    ('/largedataset', MainPage),
], debug=True)


#app = webapp2.WSGIApplication([
 #   ('/ping', MainPage),
#], debug=True)
