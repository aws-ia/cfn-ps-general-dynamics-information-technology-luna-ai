import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder
from decimal import Decimal

s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')
dbResource = boto3.resource('dynamodb')
clientEvents = boto3.client('events')
clientRk = boto3.client('rekognition')
#s3paginator = client.get_paginator('list_objects_v2')

sAssetTableName = os.environ['AssetTable']
sAssetFormatsTableName = os.environ['AssetFormatsTable']
sSearchTable = os.environ['SearchTable']
sSearchAggregateTable = os.environ['SearchAggregateTable']
sErrorTable = os.environ['ErrorTable']
sEventBusName = os.environ['EventBus_Name']
sS3RkLblDet = os.environ['S3RkLblDet']
sS3Assets = os.environ['S3Assets']
srkConfidence = os.environ['rkConfidence']
#{'version': '0', 'id': '29f9b722-81d2-11f4-c63c-74b6e0ec19bf', 'detail-type': 'ingestion', 'source': 'gdit.me', 'account': '922607582968', 'time': '2021-07-13T20:01:39Z', 'region': 'us-east-1', 'resources': ['arn:aws:s3:::me-s3ingest-d2pzq336oz8t'], 'detail': {'AssetId': 'f95db9eb149294e3d2e1892cab456274', 'AssetDatabase': 'demodb', 'AssetSize': 800714, 'AssetType': 'pdf', 'AssetStorage': 'store', 'AssetStorageBucket': '', 'AssetStorageKey': '', 'AssetURL': '', 'AssetLanguage': '', 'awsRegion': 'us-east-1', 'srcEvent': 'aws:s3', 'srcEventTime': '2021-07-13T19:12:43.172Z', 'SrcBucket': 'me-s3ingest-d2pzq336oz8t', 'SrcKey': 'The Forrester New Wave.pdf', 'SrcExtension': 'pdf', 'SrcIP': '71.163.77.185', 'SrcUser': '', 'AssetCreationTime': '1626206499'}}

def LambdaHandler(event, context):
    print(json.dumps(event))
    sSrcBucket = ""
    sSrcKey = ""
    sSrcExtension = ""
    
    #event_dict = json.loads(event) #if "key" in "dict"
    if event["detail-type"] == "ingestion":
        sSrcBucket = event["detail"]["AssetStorageBucket"]
        sSrcKey = event["detail"]["AssetStorageKey"]
        sSrcExtension = event["detail"]["AssetType"]
    #Fill in logic here for alternate format event.
    
    response = detect(event, sSrcBucket, sSrcKey, sSrcExtension)
    sKey = event["detail"]["AssetId"] + "/" + event["detail"]["AssetId"] + ".json"
    
    sEnrichments = generateEnrichments(response, "aws-detectlabels", event["detail"]["AssetId"], sS3RkLblDet, sKey)
    print(json.dumps(sEnrichments, indent=4, cls=DateTimeEncoder))
    
    #If not empty, we have an enrichment.
    if 'Enrichments' in sEnrichments:
        persistDetectiontoS3(event, response["Labels"], event["detail"]["AssetId"], sS3RkLblDet, sKey)
        enrichmentEvent(event, sEnrichments, "rekognition", "", "detectlabels", sS3RkLblDet, sKey, sSrcExtension)

    return {
        'statusCode': 200,
        'body': json.dumps('Image Text Detection Event Recorded!')
    }
        
    
#Detect image details Using AWS.
def detect(event, sBucket, sKey, sExt):
    response = clientRk.detect_labels(
        Image={
            'S3Object': {
                'Bucket': sBucket,
                'Name': sKey
            }
        },
        MinConfidence=int(srkConfidence)
    )
    return response

#Generate Mod Highlights - These are sent as "enrichments".
def generateEnrichments(response, sProcessType, sAssetId, sBucket, sKey):
    sAggregate = ""
    enrichments = []
    if response.get('Labels'):
        for item in response["Labels"]:
            enrichmentItem = {"Name": item["Name"], "ParentName": "", "Confidence": item["Confidence"]}
            enrichments.append(enrichmentItem)
            writeSearchEnrichmentDynamoDB(sProcessType, sAssetId, item["Name"], float(item["Confidence"]))
            sAggregate = sAggregate + item["Name"] + ", "
        data = {}
        data["Enrichments"] = enrichments
        writeSearchEnrichmentAggregateDynamoDB(sProcessType, sAssetId, sAggregate, response["Labels"], sBucket, sKey)
    
    return data


#Persist storage to S3 of analysis
def persistDetectiontoS3(event, response, sAssetId, sBucket, sKey):
    sContent = json.dumps(response)
    s3Client.put_object(Body=sContent, Bucket=sBucket, Key=sKey)
    
class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime)):
                return obj.isoformat()

#Write the event to the event bridge. It will kickoff other.
def enrichmentEvent(event, enrichments, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension):
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
        "ProcessStartTime": str(int(time.time())),
        "Enrichments": enrichments["Enrichments"]
    }
    
    bridgeEvent = {
        'EventBusName':sEventBusName,
        'Time': datetime.utcnow(),
        'Source':'gdit.me',
        'Resources' : [],
        'DetailType':'enrichments',
        'Detail': json.dumps(appEvent, indent=4, cls=DateTimeEncoder)
    }
    
    # Send event to EventBridge
    responseEventPublish = clientEvents.put_events(
        Entries=[
            bridgeEvent
            ]
    )
    print(json.dumps(bridgeEvent, indent=4, cls=DateTimeEncoder))
#    print(responseEventPublish)
    return responseEventPublish #allow caller to determine whether to use response id or not.
    
def writeSearchEnrichmentDynamoDB(sProcessType, sAssetId, sEnrichment, nConfidence):
    table = dbResource.Table(sSearchTable)
    responseDynamoDB = table.put_item(
        Item={
            "Term": sEnrichment,
            "Context": sProcessType + "-" + sAssetId,
            "Confidence": Decimal(nConfidence),
            "AssetId": sAssetId,
            "Timestamp": Decimal(time.time())
        }
        )
    print(json.dumps(responseDynamoDB))
    return responseDynamoDB
    
def writeSearchEnrichmentAggregateDynamoDB(sProcessType, sAssetId, sEnrichment, sFullDetail, sBucket, sKey):
    table = dbResource.Table(sSearchAggregateTable)

    responseDynamoDB = table.put_item(
        Item={
            "AssetId": sAssetId,
            "Context": sEnrichment + " ",
            "ProcessType": sProcessType,
            "FullDetail": json.dumps(sFullDetail),
            "EnrichmentBucket": sBucket,
            "EnrichmentKey": sKey,
            "Timestamp": Decimal(time.time())
        }
        )
    print(json.dumps(responseDynamoDB))
    return responseDynamoDB