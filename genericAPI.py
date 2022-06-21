import pandas as pd
import yaml
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector as sf
import os
import sys
from datetime import datetime
from datetime import datetime, timedelta, date
##import datetime
import requests
import io
from requests.exceptions import ConnectionError
import base64
import boto3
import json
from botocore.exceptions import ClientError
from pytz import timezone
from time import sleep
import ast

tz = timezone('EST')
##currentTime = datetime.datetime.now(tz=None)
currentTime = datetime.now(tz=None)

class CustomException(Exception):   
   def __init__(self, value=None, *args, **kwargs):
      self.parameter = value
      for key, value in kwargs.items():
        setattr(self, key, value)

      for key, value in self.__dict__.items():
        print("%s => %s" % ( key, value )) 

   def __str__(self):
      return repr(self.parameter)   

df = pd.DataFrame()
class genericAPI(object):

    @classmethod
    def fix_date_cols(cls, df, tz = 'UTC'):
        cols = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in cols:
           df[col] = df[col].dt.tz_localize(tz)
        return df 

    @classmethod
    def write_snowflake_data(cls, sfAccount, sfUser, sfPass, sfWarehouse, sfDatabase, sfSchema, sfTableName, df):
       try: 
          conn = sf.connect(user=sfUser,
                            password=sfPass,
                            ##authenticator='externalbrowser',
                            account=sfAccount,
                            warehouse=sfWarehouse,
                            database=sfDatabase,
                            schema=sfSchema)

          df.columns = map(lambda x: str(x).upper(), df.columns)
          df.columns = df.columns.str.replace(' ', '_')
          print('Writing data into Snowflake.')
          success, nchunks, nrows, _ = write_pandas(conn=conn, df=df, table_name=sfTableName,database=sfDatabase,schema=sfSchema)

       except Exception as e: 
          print('An Exception occured in write_snowflake_data. Please check the parameter is available', e)
          sys.exit(1)   

    @classmethod
    def df_include_landing_audit_columns(cls, df, rec_src_name):
        df['REC_SRC'] = rec_src_name
        df['LOADDTS'] = currentTime
        df['EVENTDTS'] = currentTime
        df['ROW_SQN'] = df.groupby(['LOADDTS']).cumcount()+1
        
        return df    

    @classmethod
    def read_csv_data_from_url_folder(cls, csv_url):
        try:
          df_downloaded_csv = pd.DataFrame()
          github_session = requests.Session()
          response = github_session.get(csv_url)
          download = response.content
          response_status = response.status_code

          if response_status == 200 :
             df_downloaded_csv = pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)
          else:
              raiseException = 'Invalid URL' + str(response_status)
              raise CustomException(raiseException)    
              
          return df_downloaded_csv
      
        except Exception as e:
          raise CustomException(e)
          sys.exit(1)
    
    @classmethod
    def get_dateList(cls, startDate, startMonth, startYear, endDate, endMonth, endYear):
        startDate = date(startYear, startMonth, startDate)
        endDate   = date(endYear, endMonth, endDate)
        dateslist = pd.date_range(startDate, endDate - timedelta(days=1), freq='d')
        dateslist = [x.date() for x in dateslist]
        dateslist = [date.strftime(x, "%m-%d-%Y") for x in dateslist]
        
        return dateslist
   
    @classmethod
    def getDateFromConfig(cls, sectionValue, yamlFileName):
       with open(yamlFileName, 'r') as f:
           doc = yaml.safe_load(f)
           startDate = doc[sectionValue]["startDate"]
           startMonth = doc[sectionValue]["startMonth"]
           startYear = doc[sectionValue]["startYear"]
           endDate = doc[sectionValue]["endDate"]
           endMonth = doc[sectionValue]["endMonth"]
           endYear = doc[sectionValue]["endYear"]
           UseCurrentDateFlag = doc[sectionValue]["UseCurrentDateFlag"]
        
           return startDate, startMonth, startYear, endDate, endMonth, endYear, UseCurrentDateFlag, doc
    
    @classmethod    
    def setDateToConfig(cls, dictConfig, sectionValue):
        dictConfig[sectionValue]['startDate'] = datetime.now().day
        dictConfig[sectionValue]['startMonth'] = datetime.now().month
        dictConfig[sectionValue]['startYear'] = datetime.now().year
        dictConfig[sectionValue]['endDate'] = (datetime.now() + timedelta(1)).day
        dictConfig[sectionValue]['endMonth'] = (datetime.now() + timedelta(1)).month
        dictConfig[sectionValue]['endYear'] = (datetime.now() + timedelta(1)).year
        dictConfig[sectionValue]['UseCurrentDateFlag'] = 'Y'
        return dictConfig
    
    @classmethod
    def loadFromCsv(cls, sectionValue):
       with open('loadFromCsv.yaml', 'r') as f:
           doc = yaml.safe_load(f)
           csvUrl = doc[sectionValue]["csvUrl"]
           secRecName = doc[sectionValue]["srcRecName"]
           snowflakeConnection = doc[sectionValue]["snowflakeConnection"]
        
           return csvUrl, secRecName, snowflakeConnection

    @classmethod
    def loadFromFlatFile(cls, sectionValue):

       with open('loadFromFlatFile.yaml', 'r') as f:
           doc = yaml.safe_load(f)
           csvUrl = doc[sectionValue]["flatFileName"]
           secRecName = doc[sectionValue]["srcRecName"]
           snowflakeConnection = doc[sectionValue]["snowflakeConnection"]
           seperatorSymbol = doc[sectionValue]["seperatorSymbol"]
        
           return csvUrl, secRecName, snowflakeConnection, seperatorSymbol 
      
    @classmethod
    def getExcelFileLoadInfo(cls, sectionValue):
       with open('loadFromExcel.yaml', 'r') as f:
           doc = yaml.safe_load(f)
           excelFileName = doc[sectionValue]["excelFileName"]
           srcSheet = doc[sectionValue]["srcSheet"]
           requiredColumns = doc[sectionValue]["requiredColumns"]
           srcRecName = doc[sectionValue]["srcRecName"]
           snowflakeConnection = doc[sectionValue]["snowflakeConnection"]
           
           return excelFileName, srcSheet, srcRecName, requiredColumns, snowflakeConnection

    @classmethod
    def getMoodysBasket(cls, sectionValue):
       with open('Moodys.yaml', 'r') as f:
           doc = yaml.safe_load(f)
           ACC_KEY = doc[sectionValue]["ACC_KEY"]
           ENC_KEY = doc[sectionValue]["ENC_KEY"]
           BASKET_NAME = doc[sectionValue]["BASKET_NAME"]
        
           return ACC_KEY, ENC_KEY, BASKET_NAME 

    @classmethod
    def getIHSMarkitInfo(cls, sectionValue):
       with open('ihsMarkit.yaml', 'r') as f:
           doc = yaml.safe_load(f)
           USERNAME = doc[sectionValue]["USERNAME"]
           PASSWORD = doc[sectionValue]["PASSWORD"]
        
           return USERNAME, PASSWORD    
    
    @classmethod
    def getNumeratorFileLoadInfo(cls, sectionValue):
       with open('loadFromExcel.yaml', 'r') as f:
           doc = yaml.safe_load(f)
           excelFileName = doc[sectionValue]["excelFileName"]
           srcRecName = doc[sectionValue]["srcRecName"]
           snowflakeConnection = doc[sectionValue]["snowflakeConnection"]
           startingHeaderLine = doc[sectionValue]["startingHeaderLine"]
           sheet1ColumnNumber = doc[sectionValue]['sheet1ColumnNumber']
           sheet3ColumnNumber = doc[sectionValue]['sheet3ColumnNumber']
           requiredColumns = doc[sectionValue]['requiredColumns']
        
           return excelFileName, srcRecName, snowflakeConnection, startingHeaderLine, sheet1ColumnNumber, sheet3ColumnNumber, requiredColumns


    @classmethod    
    def get_Cases_Data(cls, dateslist, base_url):
       df_final_downloaded_csv = pd.DataFrame()
       for datestring in dateslist:
          print('Processing Date: ', datestring)
          csv_url = base_url + datestring + ".csv"
          print(csv_url)
          try:
             df_downloaded_csv = genericAPI.read_csv_data_from_url_folder(csv_url) 
             df_downloaded_csv['Date'] = datetime.strptime(datestring, '%m-%d-%Y')
             df_final_downloaded_csv = df_final_downloaded_csv.append(df_downloaded_csv, ignore_index = True)
          except Exception as e:
             raise CustomException(e)
             sys.exit(1)
       return df_final_downloaded_csv

    @classmethod    
    def getAcquisitionConfig(cls, processName):
        snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genericAPI.get_snowflake_config('acquisitionConfigConnection')
        
        conn = sf.connect(user=snowflakeUser,
                          password=snowflakePass,
                          account=snowflakeAccount,
                          warehouse=snowflakeWarehouse,
                          database=snowflakeDatabase,
                          schema=snowflakeSchema)
        
        query_text = 'SELECT * FROM ' + snowflakeSchema + '.' + snowflakeTableName +';'
        query = (query_text)
        ##query=('SELECT * FROM CONFIG_TABLES.ACQUISITION_CONFIG;')
        cursor = conn.cursor()
        cursor.execute(query)
        resultset = cursor.fetchall()
        df =  pd.DataFrame(resultset)
        df.columns = ['PROCESSNAME','CURRENTDATE_FLAG','STARTDATE','ENDDATE']
        df = df.query('PROCESSNAME == @processName')
        
        processName = df['PROCESSNAME'].to_string(index=False)
        startDate = df['STARTDATE'].to_string(index=False)
        endDate = df['ENDDATE'].to_string(index=False)
        currentDateFlag = df['CURRENTDATE_FLAG'].to_string(index=False)
        
        return processName, startDate, endDate, currentDateFlag

       ## df['STARTDATE']= df['PROCESSNAME'].apply(lambda x: currentDate if (x == processName ) else x) 


    @classmethod
    def executeSnowflakeQuery(cls, query):
        snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genericAPI.get_snowflake_config('acquisitionConfigConnection')
        
        conn = sf.connect(user=snowflakeUser,
                          password=snowflakePass,
                          account=snowflakeAccount,
                          warehouse=snowflakeWarehouse,
                          database=snowflakeDatabase,
                          schema=snowflakeSchema)
        
        cursor = conn.cursor()
        cursor.execute(query)

    @classmethod
    def executeSnowflakeQueryParm(cls, query, snowflakeConnection):
        snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genericAPI.get_snowflake_config(snowflakeConnection)
        
        conn = sf.connect(user=snowflakeUser,
                          password=snowflakePass,
                          account=snowflakeAccount,
                          warehouse=snowflakeWarehouse,
                          database=snowflakeDatabase,
                          schema=snowflakeSchema)
        
        cursor = conn.cursor()
        cursor.execute(query)
            
    @classmethod    
    def get_secret(cls, secretName, regionName):
        ##secret_name = "arn:aws:secretsmanager:us-east-1:745001225527:secret:eon/dev/databases/snowflake-TDqIno"
    
        # Create a Secrets Manager client
       
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=regionName
        )
        
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secretName)
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            else:
                # Please see https://docs.aws.amazon.com/secretsmanager/latest/apireference/CommonErrors.html for all the other types of errors not handled above
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                return get_secret_value_response['SecretString']
            else:
                return base64.b64decode(get_secret_value_response['SecretBinary'])        
        
    @classmethod
    def get_snowflake_config(cls, sectionValue):
       with open('snowflakeconfig.yaml', 'r') as f:
           doc = yaml.safe_load(f)
           snowflakeSecretName = doc["snowflakeBasicConnection"]["snowflakeSecretName"]
           snowflakeRegionName = doc["snowflakeBasicConnection"]["snowflakeRegionName"]
           
           snowflakeDetails = json.loads(genericAPI.get_secret(snowflakeSecretName, snowflakeRegionName))
            
           snowflakeAccount = snowflakeDetails['account']
           snowflakeUser = snowflakeDetails['username']
           snowflakePass = snowflakeDetails['password']
           snowflakeWarehouse = snowflakeDetails['warehouse']
           snowflakeDatabase = snowflakeDetails['database']
           snowflakeSchema = doc[sectionValue]["snowflakeSchema"]
           snowflakeTableName = doc[sectionValue]["snowflakeTableName"]
        
           return snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName    

    @classmethod
    def getFredSeriesUnits(cls):
        snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genericAPI.get_snowflake_config('fredSeriesUnitsConnection')
        
        conn = sf.connect(user=snowflakeUser,
                          password=snowflakePass,
                          account=snowflakeAccount,
                          warehouse=snowflakeWarehouse,
                          database=snowflakeDatabase,
                          schema=snowflakeSchema)
        

  
        query_text = 'SELECT * FROM ' + snowflakeSchema + '.' + snowflakeTableName +';'
        query = (query_text)
        cursor = conn.cursor()
        cursor.execute(query)
        df_fredSeriesUnits = pd.DataFrame.from_records(iter(cursor), columns=[x[0] for x in cursor.description])
        
        return df_fredSeriesUnits