import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder

#clientS3 = boto3.client('s3')
#resourceS3 = boto3.resource('s3')
clientEvents = boto3.client('events')
clientDoc = boto3.resource('dynamodb')
textract_client = boto3.client('textract')

sAssetTableName = os.environ['AssetTable']
sAssetProcessingTableName = os.environ['AssetProcessingTable']
sErrorTable = os.environ['ErrorTable']
sEventBusName = os.environ['EventBus_Name']
sTxtractBucket = os.environ['S3txtract']
sTxtractABucket = os.environ['S3txtractA']
sTxtractBucketarn = os.environ['S3txtractarn']
sTxtractABucketarn = os.environ['S3txtractAarn']
sS3Assets = os.environ['S3Assets']
sTxtractSNSarn = os.environ['TextractSNSarn']
sTxtractSNSTopicarn = os.environ['TextractSNSTopicArn']
sTxtractASNSTopicarn = os.environ['TextractASNSTopicArn']

#Amazon Textract supports: English, Spanish, German, Italian, French, and Portuguese
#Amazon Textract supports (PDF, JPEG, and PNG)

#Event rules cause this to only file for pdf events.
def LambdaHandler(event, context):
    #{'version': '0', 'id': '29f9b722-81d2-11f4-c63c-74b6e0ec19bf', 'detail-type': 'ingestion', 'source': 'gdit.me', 'account': '922607582968', 'time': '2021-07-13T20:01:39Z', 'region': 'us-east-1', 'resources': ['arn:aws:s3:::me-s3ingest-d2pzq336oz8t'], 'detail': {'AssetId': 'f95db9eb149294e3d2e1892cab456274', 'AssetDatabase': 'demodb', 'AssetSize': 800714, 'AssetType': 'pdf', 'AssetStorage': 'store', 'AssetStorageBucket': '', 'AssetStorageKey': '', 'AssetURL': '', 'AssetLanguage': '', 'awsRegion': 'us-east-1', 'srcEvent': 'aws:s3', 'srcEventTime': '2021-07-13T19:12:43.172Z', 'SrcBucket': 'me-s3ingest-d2pzq336oz8t', 'SrcKey': 'The Forrester New Wave.pdf', 'SrcExtension': 'pdf', 'SrcIP': '71.163.77.185', 'SrcUser': '', 'AssetCreationTime': '1626206499'}}
    print(event)
    
    sAssetId = event["detail"]["AssetId"]
    sAssetBucket = event["detail"]["AssetStorageBucket"]
    sAssetKey = event["detail"]["AssetStorageKey"]
    sExtension = event["detail"]["AssetType"]
    startTextract(event, sAssetId, sExtension, sAssetBucket, sAssetKey)

#Start Textract jobs both normal and analysis ones. Record that processing has started.
def startTextract(event, sAssetId, sExtension, sAssetBucket, sAssetKey):
    #Textract can handle jpg, png, and pdf.
    #For our purposes, we will handle pdf only with textract.
    #We will convert jpg, png to pdf, which will then trigger this for all types.
    sPrefix = sAssetId
    
    #Source
    srcdocLoc = {
        'S3Object':{
            'Bucket': sAssetBucket,
            'Name': sAssetKey
        }
    }
    print('Bucket ' + sAssetBucket)
    print('Name ' + sAssetKey)
    
    #Destination stored under AssetId
    docDestinTextDetection = {
        "S3Bucket": sTxtractBucket,
        "S3Prefix": sPrefix
    }
    
    responseTextDetection = textract_client.start_document_text_detection(
#        ClientRequestToken = sAssetId,
        DocumentLocation = srcdocLoc,
        JobTag = sAssetId,
        NotificationChannel={
            "RoleArn": sTxtractSNSarn,
            "SNSTopicArn": sTxtractSNSTopicarn
        },
            OutputConfig = docDestinTextDetection
        )
        
    sProcessID = responseTextDetection["JobId"]
    sProcessOutputKey = "output/" + sProcessID + "/"
    
    writeDB(event, "textract", sProcessID, "Extract", sTxtractBucket, sProcessOutputKey, event["detail"]["SrcExtension"])
    writeMessageEvent(event, "textract", sProcessID, "Extract", sTxtractBucket, sProcessOutputKey, event["detail"]["SrcExtension"])
    
    #Textract Analysis Processing
    docDestinTextAnalysis = {
        "S3Bucket": sTxtractABucket,
        "S3Prefix": sPrefix
    }

# Removed, since we no longer want to do doc analysis in the demo. 
#    responseTextAnalysis = textract_client.start_document_analysis(
#        DocumentLocation = srcdocLoc,
#        FeatureTypes =["TABLES", "FORMS"],
#        JobTag = sAssetId,
#        NotificationChannel={
#            "RoleArn": sTxtractSNSarn,
#            "SNSTopicArn": sTxtractASNSTopicarn
#        },
#        OutputConfig = docDestinTextAnalysis
#    )
    
#    sProcessID = responseTextDetection["JobId"]
#    sProcessOutputKey = "output/" + sProcessID + "/"
    
#    writeDB(event, "textract", sProcessID, "ExtractAnalysis", sTxtractABucket, sProcessOutputKey, event["detail"]["SrcExtension"])
#    writeMessageEvent(event, "textract", sProcessID, "ExtractAnalysis", sTxtractABucket, sProcessOutputKey, event["detail"]["SrcExtension"])


def writeDB(event, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension):
    table = clientDoc.Table(sAssetProcessingTableName)
    responseDynamoDB = table.put_item(
        Item={
            "AssetId": event["detail"]["AssetId"],
            "ProcessId": sProcessId,
            "ComponentSource": sComponentSource,
            "ProcessType": sProcessType,
            "ProcessOutputBucket": sProcessOutputBucket,
            "ProcessOutputKey": sProcessOutputKey,
            "ProcessExtension": sProcessExtension,
            "ProcessData": "",
            "ProcessStartTime": str(int(time.time())),
            "ProcessCompletionTime": ""
        }
        )
    return responseDynamoDB

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime)):
                return obj.isoformat()

#Write the event to the event bridge. It will kickoff other.
def writeMessageEvent(event, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension):
    appEvent = {
        "AssetId": event["detail"]["AssetId"],
        "AssetDatabase": event["detail"]["AssetDatabase"],
        "AssetSize": event["detail"]["AssetSize"],
        "AssetType": event["detail"]["AssetType"],
        "AssetStorage": event["detail"]["AssetStorage"], #store or analyze
        "AssetStorageBucket": event["detail"]["AssetStorageBucket"],
        "AssetStorageKey": event["detail"]["AssetStorageKey"],
        "AssetURL": event["detail"]["AssetURL"], #URL
        "AssetLanguage": event["detail"]["AssetLanguage"],
        "ComponentSource": sComponentSource,
        "ProcessType": sProcessType,
        "ProcessId": sProcessId,
        "ProcessOutputBucket": sProcessOutputBucket,
        "ProcessOutputKey": sProcessOutputKey,
        "ProcessExtension": sProcessExtension,
        "ProcessData": "",
        "ProcessStartTime": str(int(time.time()))
    }
    
    bridgeEvent = {
        'EventBusName':sEventBusName,
        'Time': datetime.utcnow(),
        'Source':'gdit.me',
        'Resources' : [
            sTxtractBucketarn,
            sTxtractABucketarn
            ],
        'DetailType':'textractstartedasync',
        'Detail': json.dumps(appEvent, indent=4, cls=DateTimeEncoder)
    }
    
    # Send event to EventBridge
    responseEventPublish = clientEvents.put_events(
        Entries=[
            bridgeEvent
            ]
    )
#    print(json.dumps(bridgeEvent, indent=4, cls=DateTimeEncoder))
#    print(responseEventPublish)
    return responseEventPublish #allow caller to determine whether to use response id or not.