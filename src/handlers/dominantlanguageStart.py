import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder

s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')
dbResource = boto3.resource('dynamodb')
clientEvents = boto3.client('events')
comprehendClient = boto3.client('comprehend')

sAssetTableName = os.environ['AssetTable']
sAssetAttributeTableName = os.environ['AssetAttributeTable']
sAssetHistoryTableName = os.environ['AssetHistoryTable']
sAssetFormatsTableName = os.environ['AssetFormatsTable']
sErrorTable = os.environ['ErrorTable']
sEventBusName = os.environ['EventBus_Name']
sS3PlainTextBucket = os.environ['S3txtPlain']
sS3PlainTextBucketarn = os.environ['S3txtPlainarn']
sS3TxtractBucket = os.environ['S3txtract']
sS3TxtractABucket = os.environ['S3txtractA']
sS3Assets = os.environ['S3Assets']
sS3NlpLang = os.environ['S3NlpLang']
sComprehendAccessarn = os.environ['ComprehendAccessarn']

def LambdaHandler(event, context):
    srcBucket = ""
    srcKey = ""
    srcUri = ""
    destinBucket = sS3NlpLang
    destinPrefix = ""
    destinUri = ""
    sAssetId = ""
    
    if event["detail-type"] == "ingestion":
        sSrcBucket = event["detail"]["AssetStorageBucket"]
        sSrcKey = event["detail"]["AssetStorageKey"]
        sSrcExtension = event["detail"]["AssetType"]
        sAssetId = event["detail"]["AssetId"]
    elif event["detail-type"] == "alternateformat":
        sSrcBucket = event["detail"]["ProcessOutputBucket"]
        sSrcKey = event["detail"]["ProcessOutputKey"]
        sSrcExtension = event["detail"]["ProcessExtension"]
        sAssetId = event["detail"]["AssetId"]
    
    srcUri = "s3://" + sSrcBucket + "/" + sSrcKey
    destinPrefix = sAssetId + "/"
    destinUri = "s3://" + destinBucket + "/" + destinPrefix
    print("Source: " + srcUri)
    print("Destination: " + destinUri + ". File output.tar.gz")
    
    dominantlanguageStart(event, srcUri, destinUri, sComprehendAccessarn, sAssetId)

    return {
        'statusCode': 200,
        'body': json.dumps('Text Document Detected - Starting to detect dominant language!')
    }

def dominantlanguageStart(event, srcUri, destinUri, daArn, sAssetId):
    responseStartLanguageDetect = comprehendClient.start_dominant_language_detection_job(
        InputDataConfig={
            'S3Uri': srcUri,
            'InputFormat': 'ONE_DOC_PER_FILE'
        },
        OutputDataConfig={
            'S3Uri': destinUri
        },
        DataAccessRoleArn=sComprehendAccessarn,
        JobName=sAssetId #,
#        ClientRequestToken=sAssetId
        )
    print(json.dumps(responseStartLanguageDetect))