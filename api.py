def api(url):
    api="""
import requests
import hashlib 
import hmac
from datetime import datetime
from datetime import datetime, timedelta, date
import datetime
import json
import csv
import pandas as pd
from time import sleep
import io
import logging
from generalapi_logger import *
from genericAPI import genericAPI as genapi
from genericAPI import CustomException 
from pytz import timezone
import sys

now = datetime.datetime.now()
filenow = now.strftime('%Y%m%d%H%M%S')
tz = timezone('EST')
my_logger = get_logger('Process_Moodys_Data_{}.log'.format(filenow))  
pd.options.display.max_colwidth = 300

#####
# Function: Make API request, including a freshly generated signature.
#
# Arguments:
# 1. Part of the endpoint, i.e., the URL after "https://api.economy.com/data/v1/"
# 2. Your access key.
# 3. Your personal encryption key.
# 4. Optional: default GET, but specify POST when requesting action from the API.
#
# Returns:
# HTTP response object.

def api_call(url,apiCommand, accKey, encKey, call_type="GET"):

  url = url + apiCommand
  print(url)
  timeStamp = datetime.datetime.strftime(
    datetime.datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
  payload = bytes(accKey + timeStamp, "utf-8")
  signature = hmac.new(bytes(encKey, "utf-8"), payload, digestmod=hashlib.sha256)
  head = dict("AccessKeyId":accKey, 
          "Signature":signature.hexdigest(), 
          "TimeStamp":timeStamp)

  if call_type == "POST":
    response = requests.post(url, headers=head)
    
  elif call_type =="DELETE":
    response = requests.delete(url, headers=head)
  else:
    response = requests.get(url, headers=head)
    
  return(response)"""
    return api

t=api('success')
print(t)
with open('api_generator.py','w') as f:
    f.write(t)