# Python 3
import http.client
import mimetypes
import base64
import json
import keyring
import datetime
import csv
import pandas as pd
import sys
from io import StringIO
import pyodbc
import sqlalchemy
import math

APPLICATION = "WFM-Reports"
VENDOR = "WFM"
BUSINESS_UNIT_NO = "4597472"
AUTHCODE = base64.b64encode(
    (APPLICATION + "@" + VENDOR + ":" + BUSINESS_UNIT_NO).encode()
  ).decode()
SERVICENAME = APPLICATION + "@" + VENDOR
FILEPATH = "cxOneToken.json"
API_VERSION = "v17.0"
PROGRESS = {
  0: '|',
  1: '/',
  2: '-',
  3: '\\',
}


def CreateNewToken() -> dict:
  """
  Obtains a new token from cxOne, returns the values as a dict.
  """
  print("Generating new token...")
  username = "mabidi@aseaglobal.com"
  password = keyring.get_password(service_name = SERVICENAME, username = username)
  # Get connection to token base URL
  conn = http.client.HTTPSConnection("api.incontact.com")
  # Create headers and payload
  headers = {
      "Authorization" : "basic " + AUTHCODE,
      "Content-type" : "application/json",
      "Cookie" : "BIGipServerpool_api=",
  }
  payload = {
    "grant_type" : "password",
    "username" : username,
    "password" : password,
    "scope" : "",
  }
  payload = json.dumps(payload)
  # Send request and process response
  conn.request("POST", "/InContactAuthorizationServer/Token", payload, headers)
  response = conn.getresponse()
  if response.getcode() != 200:
    error = "Error getting new token: " + response.getcode()
    error += "\nHeaders: " + response.getheaders()
    error += "\nBody: " + response.read()
    raise Exception(error)
  data = response.read()
  cxOneToken = ParseSaveTokenResponse(data)
  response.close()
  conn.close()
  return cxOneToken



def RefreshToken(cxOneToken: dict):
  """
  Refreshes a previously expired token. If the response is not 200, then 
  it will call CreateNewToken
  """
  print("Refreshing Token...")
  parsedUrl = UrlParser(cxOneToken['refresh_token_server_uri'])
  conn = http.client.HTTPSConnection(parsedUrl['domain'])
  # Create headers and payload
  headers = {
      "Authorization" : "basic " + AUTHCODE,
      "Content-type" : "application/json",
      "Cookie" : "BIGipServerpool_api=",
  }
  payload = {
    "grant_type": "refresh_token",
    "refresh_token": cxOneToken['refresh_token']
  }
  payload = json.dumps(payload)
  conn.request("POST", parsedUrl['path'], payload, headers)
  response = conn.getresponse()
  if response.getcode() != 200:
    print("Token refresh failed! Attempting to generate a new token...")
    cxOneToken = CreateNewToken()
  else:
    print("Token refresh successful!")
    data = response.read()
    cxOneToken = ParseSaveTokenResponse(data)
  return cxOneToken



def RetrieveCheckToken() -> dict:
  cxOneToken = {}
  # Open file that contains previous token. If the token is expired but the refresh
  # token isn't, it refreshes the token. If one does not exist or the refresh is expired, 
  # CreateNewToken is called. 
  try:
    with open(FILEPATH, 'r') as f:
      cxOneToken = json.load(f)
      expirey = datetime.datetime.strptime(
        cxOneToken['expirey'], '%Y-%m-%d %H:%M:%S.%f')
      now = datetime.datetime.utcnow()
      if now > expirey:
        # Token is expired, now checking if refresh token is expired
        refresh_expirey = expirey + datetime.timedelta(seconds = 3600)
        if now > refresh_expirey:
          # Token and Refresh are both expired, will need to get a new token
          cxOneToken = CreateNewToken()
        else:
          # Refresh token is still good, so just needs to be refreshed.
          cxOneToken = RefreshToken(cxOneToken)
  except FileNotFoundError:
    cxOneToken = CreateNewToken()
  return cxOneToken



def ParseSaveTokenResponse(data: str) -> dict:
  """
  Receives response from either a token request or a token refresh in 
  a string. It saves the response as a json file, then returns a dictionary 
  """
  cxOneToken = json.loads(data)
  start = datetime.datetime.utcnow()
  # Set expiration to 5 seconds before to allow for at least 1 API call
  expirey = start + datetime.timedelta(
    seconds = cxOneToken['expires_in'] - 5)
  cxOneToken['resource_server_base_uri'] += "services/" + API_VERSION + "/"
  cxOneToken['start'] = start.strftime('%Y-%m-%d %H:%M:%S.%f')
  cxOneToken['expirey'] = expirey.strftime('%Y-%m-%d %H:%M:%S.%f')
  with open(FILEPATH, 'w') as f:
    json.dump(cxOneToken, f, indent = 4)
  return cxOneToken



def UrlParser(wholeUrl: str) -> dict:
  dblSlashIdx = wholeUrl.find('//')
  protocol = wholeUrl[: dblSlashIdx - 1]
  domainWithPath = wholeUrl[dblSlashIdx + 2:]
  sglSlashIdx = domainWithPath.find('/')
  domain = domainWithPath[:sglSlashIdx]
  path = domainWithPath[sglSlashIdx:]
  return {
    'protocol' : protocol,
    'domain' : domain,
    'path' : path,
  }



def PrintProgress(text: str, endProgress = False) -> str:
  returnText = text
  text = ''
  while len(returnText) > 80:
    text += returnText[:80] + '\n'
    returnText = returnText[80:]
  text += '\r' + returnText
  if endProgress:
    text += '\n'
  sys.stdout.write(text)
  sys.stdout.flush()
  return returnText
  


def StartReportingJob(reportId: str) -> str:
  """
  Starts a reporting job, returns jobId in string format. While the jobId
  appears to be an int, the return type remains a string in case this changes
  in the future.
  """
  # Check if token exists, refresh/ generate new token as needed
  cxOneToken = RetrieveCheckToken()
  print("Starting job to run report number " + reportId + "...")
  parsedUrl = UrlParser(cxOneToken['resource_server_base_uri'])
  targetPath = parsedUrl['path'] + "report-jobs/" + reportId
  conn = http.client.HTTPSConnection(parsedUrl['domain'])
  payload = {
    "fileType" : "CSV",
    "includeHeaders" : "true",
    "deleteAfter" : 7,
  }
  payload = json.dumps(payload)
  headers = {
    'Authorization': 'Bearer ' + cxOneToken['access_token'],
    'Content-Type': 'application/json',
    "Cookie" : "BIGipServerpool_api=",
    "Accept" : "*/*",
    'Connection' : 'keep-alive'
  }
  conn.request("POST", targetPath, payload, headers)
  response = conn.getresponse()
  data = response.read()
  data = json.loads(data)
  response.close()
  conn.close()
  return data['jobId']



def GetReportingJobInfo(jobId: str) -> dict:
  """
  Keeps checking running job every 1/10th second until either it returns the report URL or
  it fails. Times out after 10 minutes
  """
  # Check if token exists, refresh/ generate new token as needed
  cxOneToken = RetrieveCheckToken()
  print("Checking job status...")
  parsedUrl = UrlParser(cxOneToken['resource_server_base_uri'])
  targetPath = parsedUrl['path'] + "report-jobs/" + jobId
  conn = http.client.HTTPSConnection(parsedUrl['domain'])
  payload = {}
  headers = {
    'Authorization': 'Bearer ' + cxOneToken['access_token'],
    'Content-Type': 'application/json',
    "Cookie" : "BIGipServerpool_api=",
    "Accept" : "*/*",
    'Connection' : 'keep-alive'
  }
  jobState = ''
  data = {}
  statusCode = 200
  now = start = datetime.datetime.utcnow()
  timeout = start + datetime.timedelta(minutes = 5)
  outNum = 0
  print("Time\t\t\tStatus Code\t\tJob State")
  while jobState != "Finished" \
    and statusCode < 300 \
    and statusCode >= 200 \
    and now < timeout:
    # First, print out the progress animation, then add 1 the progress number
    PrintProgress(PROGRESS.get(outNum))
    outNum += 1
    outNum = outNum % 4
    conn.request("GET", targetPath, payload, headers)
    response = conn.getresponse()
    data = response.read()
    data = json.loads(data)
    if jobState != data['jobResult']['state']:
      jobState = data['jobResult']['state']
      consoleStr = now.strftime('%Y-%m-%d %H:%M:%S') + '\t'
      consoleStr += str(response.getcode()) + '\t\t\t' + jobState
      print('\r' + consoleStr)
    # time.sleep(.1)
    now = datetime.datetime.utcnow()
  print("Job Completed")
  return data['jobResult']



def GetFinishedReport(jobResult: dict) -> pd.DataFrame:
  """
  Downloads the report that was just generated. Puts the file into a pandas dataframe,
  removes null columns, and saves the file to the disk. Returns a dataframe that then can
  be used to insert rows into database.
  """
  # Check if token exists, refresh/ generate new token as needed
  cxOneToken = RetrieveCheckToken()
  print("Retrieving File...")
  parsedUrl = UrlParser(jobResult['resultFileURL'])
  conn = http.client.HTTPSConnection(parsedUrl['domain'])
  payload = ''
  headers = {
    'Authorization': 'Bearer ' + cxOneToken['access_token'],
    'Content-Type': 'application/json',
    "Cookie" : "BIGipServerpool_api=",
    "Accept" : "*/*",
    'Connection' : 'keep-alive'
    }
  conn.request("GET", parsedUrl['path'], payload, headers)
  response = conn.getresponse()
  #pp.pprint(res.getheaders())
  rawData = response.read()
  rawData = json.loads(rawData)
  fileName = rawData['files']['fileName'].replace(' ', '_')
  rawFile = rawData['files']['file']
  rawFile = rawFile.encode()
  rawFile = base64.decodebytes(rawFile)
  rawFile = rawFile.decode('utf-8-sig')
  rawFile = rawFile.replace('\r\n\r\n', '\r\n').replace('\r\n', '\n')
  rawFile = rawFile[:-1]
  rawFile = rawFile[:rawFile.rfind('\n')]
  rawFile = StringIO(rawFile)
  fileDf = pd.read_csv(rawFile)
  fileDf = fileDf.dropna(axis = 'columns', how = 'all')
  fileDf.columns = fileDf.columns.str.replace(' ', '_')
  d= datetime.datetime.today()
  sun_offset = (d.weekday() - 6) % 7
  sunday_same_week = d - datetime.timedelta(days=sun_offset)
  td = datetime.timedelta(days=7)
  Report_Start_Date = (sunday_same_week - td).strftime("%Y-%m-%d")
  fileDf.insert(0,"Report_Start_Date",Report_Start_Date)
  fileNameList = fileName.split("_")
  fileNameList[-1] = (sunday_same_week - td).strftime("%Y-%m-%d.csv")
  fileName = '_'.join(fileNameList)
  with open(fileName, 'w') as f:
    fileDf.to_csv(f, line_terminator = '\n', index = False)
  print("File retrieval success")
  return fileDf


if __name__ == "__main__":
  reportId = '1073741981'
  jobId = StartReportingJob(reportId)
  print("Job started, Job ID: " + jobId)
  jobResult = GetReportingJobInfo(jobId)
  jobResult['fileName'] = jobResult['fileName'].replace(" ", "_")
  print("Filename: " + jobResult['fileName'])
  fileDf = GetFinishedReport(jobResult)
  # Insert rows from report into table
  engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
  fileDf2 = pd.read_sql_query('SELECT*FROM ASEA_REPORTS_STAGE.dbo.Raw_Data_Supervisor_Snapshot', engine)

  list1 = fileDf['Report_Start_Date'].tolist()
  fileDfSQL = fileDf2['Report_Start_Date']
  list2 = fileDfSQL.tolist()

  listSQL = []
  for i in list2:
      i=i.strftime('%F')
      listSQL.append(i)

  result = any(elem in listSQL for elem in list1) 
  if result:
    fileDf.drop(fileDf.index, inplace=True)
    print("The Dataframe coming from the API has already been inserted into the Table")


  else:
    numRows = len(fileDf.index)
    numCols = len(fileDf.columns)
    tableName = "Raw_Data_Supervisor_Snapshot"
    print("Inserting " + str(numRows) + " rows into " + tableName)
    chunksize = math.floor(2100 / numCols) - 1
    fileDf.to_sql(tableName, engine, 
                if_exists = 'append', index = False, 
                chunksize = chunksize, method = 'multi')

  #reportId = '10418'
  #jobId = StartReportingJob(reportId)
  #print("Job started, Job ID: " + jobId)
  #jobResult = GetReportingJobInfo(jobId)
  #jobResult['fileName'] = jobResult['fileName'].replace(" ", "_")
  #print("Filename: " + jobResult['fileName'])
  #fileDf = GetFinishedReport(jobResult)
  # Open connection to SQL Server database
  # Insert rows from report into table

  reportId = '12444'
  jobId = StartReportingJob(reportId)
  print("Job started, Job ID: " + jobId)
  jobResult = GetReportingJobInfo(jobId)
  jobResult['fileName'] = jobResult['fileName'].replace(" ", "_")
  print("Filename: " + jobResult['fileName'])
  fileDf = GetFinishedReport(jobResult)
  # Open connection to SQL Server database
  # Insert rows from report into table
  engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
  fileDf2 = pd.read_sql_query('SELECT*FROM ASEA_REPORTS_STAGE.dbo.ACW_Actual', engine)

  list1 = fileDf['Report_Start_Date'].tolist()
  fileDfSQL = fileDf2['Report_Start_Date']
  list2 = fileDfSQL.tolist()

  listSQL = []
  for i in list2:
      i=i.strftime('%F')
      listSQL.append(i)

  result = any(elem in listSQL for elem in list1) 
  if result:
    fileDf.drop(fileDf.index, inplace=True)
    print("The Dataframe coming from the API has already been inserted into the Table")


  else:
    numRows = len(fileDf.index)
    numCols = len(fileDf.columns)
    tableName = "ACW_Actual"
    print("Inserting " + str(numRows) + " rows into " + tableName)
    chunksize = math.floor(2100 / numCols) - 1
    fileDf.to_sql(tableName, engine, 
                if_exists = 'append', index = False, 
                chunksize = chunksize, method = 'multi')