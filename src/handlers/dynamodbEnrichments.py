import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder
from decimal import Decimal

clientEvents = boto3.client('events')
clientDoc = boto3.resource('dynamodb')
sAssetTableName = os.environ['AssetTable']
sAssetEnrichmentsTableName = os.environ['AssetEnrichmentsTable']

def LambdaHandler(event, context):
    print(json.dumps(event))
    writeEventToDynamoDB(event, context, sAssetEnrichmentsTableName)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Enrichment Event Captured and now recorded to DynamoDB')
    }
    
#Write the asset to the dynamodb table. Which also provides a stream of this content.
def writeEventToDynamoDB(event, context, sTableName):
    table = clientDoc.Table(sTableName)
    responseDynamoDB = table.put_item(
        Item={
            "AssetId": event["detail"]["AssetId"],
            "source": event["source"],
            "detail-type": event["detail-type"],
            "ComponentSource": event["detail"]["ComponentSource"],
            "AssetDatabase": event["detail"]["AssetDatabase"],
            "AssetURL": event["detail"]["AssetURL"],
            "AssetSize": event["detail"]["AssetSize"],
            "AssetStorage": event["detail"]["AssetStorage"],
            "ProcessData": event["detail"]["ProcessData"],
            "ProcessExtension": event["detail"]["ProcessExtension"],
            "AssetType": event["detail"]["AssetType"],
            "AssetStorageKey": event["detail"]["AssetStorageKey"],
            "ProcessType": event["detail"]["ProcessType"],
            "AssetStorageBucket": event["detail"]["AssetStorageBucket"],
            "ProcessStartTime": event["detail"]["ProcessStartTime"],
            "ProcessId": event["detail"]["ProcessId"],
            "AssetLanguage": event["detail"]["AssetLanguage"],
            "ProcessOutputKey": event["detail"]["ProcessOutputKey"],
            "Enrichments": json.loads(json.dumps(event["detail"]["Enrichments"]), parse_float=Decimal),
            "id": event["id"],
            "EventTime": event["time"],
            "Timestamp": Decimal(time.time())
        }
        )
    print(json.dumps(responseDynamoDB))
    return responseDynamoDB

#placeholder