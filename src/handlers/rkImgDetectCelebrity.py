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
sS3RkCeleb = os.environ['S3RkCeleb']
sS3Assets = os.environ['S3Assets']
srkConfidence = os.environ['rkConfidence']

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
    #print(json.dumps(response, indent=4, cls=DateTimeEncoder))
    
    sEnrichments = generateEnrichments(response, "aws-recognizecelebrities", event["detail"]["AssetId"], sS3RkCeleb, sKey)
    #print(json.dumps(sEnrichments, indent=4, cls=DateTimeEncoder))
    
    #If not empty, we have an enrichment.
    #if 'Enrichments' in sEnrichments:
    persistDetectiontoS3(event, response["CelebrityFaces"], event["detail"]["AssetId"], sS3RkCeleb, sKey)
    enrichmentEvent(event, sEnrichments, "rekognition", "", "recognizeceleb",sS3RkCeleb, sKey, sSrcExtension)

    return {
        'statusCode': 200,
        'body': json.dumps('Image Face Detection & Analysis Event Recorded!')
    }
        
    
#Detect image details Using AWS.
def detect(event, sBucket, sKey, sExt):
    response = clientRk.recognize_celebrities(
        Image={
            'S3Object': {
                'Bucket': sBucket,
                'Name': sKey
            }
        }
    )
    return response

#Generate Mod Highlights - These are sent as "enrichments".
def generateEnrichments(response, sProcessType, sAssetId, sBucket, sKey):
    sAggregate = ""
    enrichedPersons = []
    lstInclusions = ["Name", "MatchConfidence", "Id", "Urls"]
        
    for item in response["CelebrityFaces"]:
        enrichments = {}
        for k, v in item.items():
            if k in lstInclusions:
                enrichments[k] = v
        enrichedPersons.append(enrichments)
        writeSearchEnrichmentDynamoDB(sProcessType, sAssetId, item["Name"], float(item["MatchConfidence"]))
        sAggregate = sAggregate + item["Name"] + ", "
        
    data = {}
    data["Enrichments"] = enrichedPersons
    writeSearchEnrichmentAggregateDynamoDB(sProcessType, sAssetId, sAggregate, response["CelebrityFaces"], sBucket, sKey)
    
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