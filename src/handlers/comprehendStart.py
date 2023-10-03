import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder
from boto3.dynamodb.conditions import Key, Attr

s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')
dbResource = boto3.resource('dynamodb')
clientEvents = boto3.client('events')
clientComprehend = boto3.client('comprehend')
#s3paginator = client.get_paginator('list_objects_v2')

sAssetTableName = os.environ['AssetTable']
sAssetAttributeTableName = os.environ['AssetAttributeTable']
sAssetHistoryTableName = os.environ['AssetHistoryTable']
sAssetFormatsTableName = os.environ['AssetFormatsTable']
sErrorTable = os.environ['ErrorTable']
sEventBusName = os.environ['EventBus_Name']
sS3PlainTextBucket = os.environ['S3txtPlain']
sS3PlainTextBucketarn = os.environ['S3txtPlainarn']
sS3NlpNer = os.environ['S3NlpNer']
sS3Assets = os.environ['S3Assets']
sComprehendAccessarn = os.environ['ComprehendAccessarn']


def LambdaHandler(event, context):
    sSrcBucket = ""
    sSrcKey = ""

    destinBucket = sS3NlpNer
    destinPrefix = ""
    destinUri = ""
    sAssetId = event["detail"]["AssetId"]
    
    assetTable = ""
    altAssetTable = ""
    
    try:
        assetTable = LoadAssetIdDB(sAssetId)
    except (ValueError, TypeError):
        print("Asset Table Load Error")
        
    try:
        altAssetTable = LoadAlternateAssetIdDB(sAssetId, "txt")
    except (ValueError, TypeError):
        print("Asset Table Load Error")
        
    if assetTable["AssetType"] == "txt":
        sSrcBucket = assetTable["AssetStorageBucket"]
        sSrcKey = assetTable["AssetStorageKey"]
    elif altAssetTable["AssetType"] == "txt":
        sSrcBucket = altAssetTable["AssetStorageBucket"]
        sSrcKey = altAssetTable["AssetStorageKey"]
        
    destinPrefix = event["detail"]["AssetId"] + "/"
    
    print(sSrcBucket)
    print(sSrcKey)
    print(destinBucket)
    print(destinPrefix)
    print(event["detail"]["AssetLanguage"])
    entitiesDetection(sSrcBucket, sSrcKey, destinBucket, destinPrefix, event["detail"]["AssetLanguage"], sAssetId)

    return {
        'statusCode': 200,
        'body': json.dumps('Started NER Detection!')
    }

    
def entitiesDetection(srcBucket, srcKey, destinBucket, destinPrefix, sLanguage, sAssetId):
    srcUri = "s3://" + srcBucket + "/" + srcKey
    destinUri = "s3://" + destinBucket + "/" + destinPrefix
    print(srcUri)
    print(destinUri)
    responseStartEntities = clientComprehend.start_entities_detection_job(
        InputDataConfig={
            'S3Uri': srcUri,
            'InputFormat': 'ONE_DOC_PER_FILE'
        },
        OutputDataConfig={
            'S3Uri': destinUri
        },
        DataAccessRoleArn=sComprehendAccessarn,
        JobName=sAssetId,
        LanguageCode=sLanguage
        )
    print(json.dumps(responseStartEntities))
    return responseStartEntities

#Grab DynamoDB item
def LoadAlternateAssetIdDB(sAssetId, sFormatType):
    table = dbResource.Table(sAssetFormatsTableName)
    try:
        response = table.get_item(Key={
            'AssetId': sAssetId,
            'FormatType': sFormatType
        })
    except Exception as e:
        print(str(e))
    else:
        return response['Item']
        
#Grab DynamoDB item
def LoadAssetIdDB(sAssetId):
    table = dbResource.Table(sAssetTableName)
    try:
        response = table.get_item(Key={'AssetId': sAssetId})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime)):
                return obj.isoformat()
    
#Write the event to the event bridge. It will kickoff other activities.
def writeMessageEvent(event, objAssetDB, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension, sDominantLanguages):
    appEvent = {
        "AssetId": objAssetDB["AssetId"],
        "AssetDatabase": objAssetDB["AssetDatabase"],
        "AssetSize": objAssetDB["AssetSize"],
        "AssetType": objAssetDB["AssetType"],
        "AssetStorage": objAssetDB["AssetStorage"], #store or analyze
        "AssetStorageBucket": objAssetDB["AssetStorageBucket"],
        "AssetStorageKey": objAssetDB["AssetStorageKey"],
        "AssetURL": objAssetDB["AssetURL"], #URL
        "AssetLanguage": sDominantLanguages,
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
#    print(json.dumps(bridgeEvent, indent=4, cls=DateTimeEncoder))
#    print(responseEventPublish)
    return responseEventPublish #allow caller to determine whether to use response id or not.