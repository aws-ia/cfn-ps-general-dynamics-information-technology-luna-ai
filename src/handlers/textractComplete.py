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
#s3paginator = client.get_paginator('list_objects_v2')

sAssetTableName = os.environ['AssetTable']
sAssetFormatsTableName = os.environ['AssetFormatsTable']
sErrorTable = os.environ['ErrorTable']
sEventBusName = os.environ['EventBus_Name']
sS3PlainTextBucket = os.environ['S3txtPlain']
sS3PlainTextBucketarn = os.environ['S3txtPlainarn']
sTxtractBucket = os.environ['S3txtract']
sTxtractABucket = os.environ['S3txtractA']
sS3Assets = os.environ['S3Assets']
sTxtractSNSTopicarn = os.environ['TextractSNSTopicArn']
sTxtractASNSTopicarn = os.environ['TextractASNSTopicArn']

def LambdaHandler(event, context):

    print(json.dumps(event))
    print(context)

    #Loop over the records here and send them for execution by proper set.
    for rec in event["Records"]:
        if rec["Sns"]["TopicArn"] == sTxtractSNSTopicarn:
            TextractTextDetection(rec, event, context)
        elif rec["Sns"]["TopicArn"] == sTxtractASNSTopicarn:
            TextractTextAnalysis(rec, event, context)

    return {
        'statusCode': 200,
        'body': json.dumps('Processed Textract Records!')
    }

def TextractTextDetection(rec, event, context):
    message = json.loads(rec["Sns"]["Message"])
    sProcessID = message["JobId"]
    sStatus = message["Status"]
    sAPI = message["API"]
    sAssetID = message["JobTag"] #I pass the AssetID into the job tag when I start the job, so you have it here in the message.
    tEventTimeStamp = message["Timestamp"]
    
    if sStatus == "SUCCEEDED":
        sOrgPrefix = sAssetID + "/" + sProcessID + "/"
        sNewPrefix = sAssetID + "/"
        moveDirectory(sTxtractBucket, sOrgPrefix, sTxtractBucket, sNewPrefix) #remove process ID from the path, delete the old records.
        sPlainTextKey = sNewPrefix + sAssetID + ".txt"
        generatePlainText(sTxtractBucket, sNewPrefix, sS3PlainTextBucket, sPlainTextKey)
        
        objAssetDB = LoadAssetIdDB(sAssetID)
        writeAlternateAssetToDynamoDB(objAssetDB, event, sS3PlainTextBucket, sPlainTextKey, "txt")
        writeMessageEvent(event, objAssetDB, "textract", sProcessID, "extract", sS3PlainTextBucket, sPlainTextKey, "txt")
    else:
        return

def LoadAssetIdDB(sAssetId):
    table = dbResource.Table(sAssetTableName)
    try:
        response = table.get_item(Key={'AssetId': sAssetId})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']
  
def TextractTextAnalysis(rec, event, context):
    print("Entered TextractTextAnalysis Block [Not Implemented]")
    message = json.loads(rec["Sns"]["Message"])
    sProcessID = message["JobId"]
    sStatus = message["Status"]
    sAPI = message["API"]
    sAssetID = message["JobTag"] #I pass the AssetID into the job tag when I start the job, so you have it here in the message.
    tEventTimeStamp = message["Timestamp"]
    
    if sStatus == "SUCCEEDED":
        sOrgPrefix = sAssetID + "/" + sProcessID + "/"
        sNewPrefix = sAssetID + "/"
        moveDirectory(sTxtractABucket, sOrgPrefix, sTxtractABucket, sNewPrefix) #remove process ID from the path, delete the old records.
        #sPlainTextKey = sNewPrefix + sAssetID + ".txt"
    else:
        return

#Read in textract and generate plaintext output.
def generatePlainText(srcBucket, srcPrefix, newBucket, newKey):
    orgBucketS3 = s3Resource.Bucket(srcBucket)
    objects = orgBucketS3.objects.filter(Prefix=srcPrefix)
    lines = "" #scope is across objects
    
    for obj in objects:
        objKey = str(obj.key) 
        
        objContent = s3Client.get_object(Bucket=srcBucket, Key=objKey)
        print(objKey)
        response = json.loads(objContent['Body'].read())
        print(response)
        # response['Body'].read().decode('utf-8') #if decoding needed
        
        # Detect columns and print lines
        for item in response["Blocks"]:
            if item["BlockType"] == "LINE":
                lines = lines + item["Text"] + "\n"

    s3Client.put_object(Body=lines, Bucket=newBucket, Key=newKey)

#move a directory to another location on S3.
def moveDirectory(orgBucket, orgPrefix, newBucket, newPrefix):
    orgBucketS3 = s3Resource.Bucket(orgBucket)
    objects = orgBucketS3.objects.filter(Prefix=orgPrefix)
    for obj in objects:
        objKey = str(obj.key)
        srcExtension = objKey.split(".")[-1]
        srcFileName = objKey.split("/")[-1]
        
        print(orgBucket)
        print(orgPrefix)
        print(objKey)
        print(newBucket)
        print(newPrefix)
        
        #Delete access check files. Otherwise move files
        if srcExtension == "s3_access_check":
            deleteItem(orgBucket, objKey)
        else:
            sNewKey = newPrefix + srcFileName
            moveItem(orgBucket, objKey, newBucket, sNewKey)

#move items in directory
def moveItem(orgBucket, orgKey, newBucket, newKey):
    copy_source = {
        'Bucket': orgBucket,
        'Key': orgKey
    }
    try:
        s3Resource.meta.client.copy(copy_source, Bucket = newBucket, Key = newKey)
        deleteItem(bucket = orgBucket, key = orgKey)
    except (ValueError, TypeError):
        json.dumps('File Move Error!')    

def deleteItem(bucket, key):
    try:
        s3Client.delete_object(Bucket=bucket, Key=key)
        json.dumps('Deleted ' + bucket + " " + key)
    except (ValueError, TypeError):
        json.dumps('File Deletion Error!') 

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime)):
                return obj.isoformat()

#Write the event to the event bridge. It will kickoff other.
def writeMessageEvent(event, objAssetDB, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension):
    appEvent = {
        "AssetId": objAssetDB["AssetId"],
        "AssetDatabase": objAssetDB["AssetDatabase"],
        "AssetSize": objAssetDB["AssetSize"],
        "AssetType": objAssetDB["AssetType"],
        "AssetStorage": objAssetDB["AssetStorage"], #store or analyze
        "AssetStorageBucket": objAssetDB["AssetStorageBucket"],
        "AssetStorageKey": objAssetDB["AssetStorageKey"],
        "AssetURL": objAssetDB["AssetURL"], #URL
        "AssetLanguage": objAssetDB["AssetLanguage"],
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
            sS3PlainTextBucketarn
            ],
        'DetailType':'alternateformat',
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

#Write the alternate asset to the dynamodb table. Which also provides a stream of this content.
def writeAlternateAssetToDynamoDB(objAssetDB, event, srcBucket, srcKey, srcExtension):
    table = dbResource.Table(sAssetFormatsTableName)
    responseDynamoDB = table.put_item(
        Item={
            "AssetId": objAssetDB["AssetId"],
            "FormatType": srcExtension,
            "AssetDatabase": objAssetDB["AssetDatabase"],
            "AssetSize": 0,
            "AssetType": srcExtension,
            "AssetStorage": "store", #store or analyze
            "AssetStorageBucket": srcBucket,
            "AssetStorageKey": srcKey,
            "AssetURL": "", #URL
            "AssetLanguage": ""
        }
        )
    return responseDynamoDB


# Lambda receives events from Textract Text Detection AND Text Analysis, so there are two incoming event types. Both are below.        
# 
# Text Detection Below....
#{
#    "Records": [
#        {
#            "EventSource": "aws:sns",
#            "EventVersion": "1.0",
#            "EventSubscriptionArn": "arn:aws:sns:us-east-1:922607582968:me-AmazonTextractSNS-18HDM71YTLFXZ:7377aa6f-489d-432e-a63f-2c93b32307f8",
#            "Sns": {
#                "Type": "Notification",
#                "MessageId": "aa83b885-6af9-5412-ad92-e1f52d9f25d5",
#                "TopicArn": "arn:aws:sns:us-east-1:922607582968:me-AmazonTextractSNS-18HDM71YTLFXZ",
#                "Subject": null,
#                "Message": "{\"JobId\":\"a14463359e886b62936e0ce46f52f4c8658c20ddd3d5d69d0d528acadb114f92\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentTextDetection\",\"JobTag\":\"f95db9eb149294e3d2e1892cab456274\",\"Timestamp\":1626752294178,\"DocumentLocation\":{\"S3ObjectName\":\"f95db9eb149294e3d2e1892cab456274/f95db9eb149294e3d2e1892cab456274.pdf\",\"S3Bucket\":\"me-s3assets-1a0jkn6mzi0fk\"}}",
#                "Timestamp": "2021-07-20T03:38:14.221Z",
#                "SignatureVersion": "1",
#                "Signature": "gOg/OFLLw9D4E5KcmWb8nS97DHe/QIH29KvwUqvKAn6zjB/C7xIrnpt1FrjG6xjqt2jZ0Am9zBTSauEPburiA1jBEaEIkJBumC6+Mqa35G1zahLrpKxbEjHE4yQOYXHoWkY2mf8+zsrvj+TL6ogsFQ31uQGiVr3PWydZvXdyv8yF0Q97+asT7p9tHo2/3cmEWFUsSUksHB1RosGBSYQJQSlhbqJ0f/1D6ja8C0sTc9jiMtw6bSJixT6kp7iCJ+H3qtIgqfPFTDU7SUHei0W2VOXz1WT3TwT/QmpwNLVd/YLSL99yIbCZqUCdUTAgpb56x2/wFRRT4sVnFzx/t9MLEw==",
#                "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem",
#                "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:922607582968:me-AmazonTextractSNS-18HDM71YTLFXZ:7377aa6f-489d-432e-a63f-2c93b32307f8",
#                "MessageAttributes": {}
#            }
#        }
#    ]
#}

# Text Analysis
#{
#    "Records": [
#        {
#            "EventSource": "aws:sns",
#            "EventVersion": "1.0",
#            "EventSubscriptionArn": "arn:aws:sns:us-east-1:922607582968:me-AmazonTextractASNS-11QV1D7FH37EF:8237a5f6-bac2-4fc2-a8f6-1dcedd6ee31f",
#            "Sns": {
#                "Type": "Notification",
#                "MessageId": "e29d2231-865a-5caa-bb58-7b41c1347a43",
#                "TopicArn": "arn:aws:sns:us-east-1:922607582968:me-AmazonTextractASNS-11QV1D7FH37EF",
#                "Subject": null,
#                "Message": "{\"JobId\":\"c8ea0be087f17edaac0cb487267a4fcd0a2f572abbd91f7f9057bb958696e241\",\"Status\":\"SUCCEEDED\",\"API\":\"StartDocumentAnalysis\",\"JobTag\":\"f95db9eb149294e3d2e1892cab456274\",\"Timestamp\":1626752302684,\"DocumentLocation\":{\"S3ObjectName\":\"f95db9eb149294e3d2e1892cab456274/f95db9eb149294e3d2e1892cab456274.pdf\",\"S3Bucket\":\"me-s3assets-1a0jkn6mzi0fk\"}}",
#                "Timestamp": "2021-07-20T03:38:22.726Z",
#                "SignatureVersion": "1",
#                "Signature": "cmL4d3LdfPrQxAJKN/Fa08ACpyZsDk4dSz47dU+k85gjUECovpO70V6wP7QtO5F0rQVnU1H+yoZ3aCHDGwY630LYeD3/++t0vWA/7CrRxeLnlbmXg6Vb0Pl3DM2yDUA8Wx/i+MHM23K6Wph8mdVqJQlVyXIJJiZUifYbdFFb+9RqipxIEjD+Lm7I5EOHmZuDMTmjrhC/Jl0Cu2ZTXv/bSvSKRU08ay9g2J9OUeMA/BsrHs4Zp44hTPOYniSxhkmZT8BcDKTzjUhCRRJnXsMsNyOXqHUd/b4EIPwYo7jTWwEjDD8oFJKvJW0pU59s9OP5Jnq5ufRuH+CFICCe0GYa6A==",
#                "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem",
#                "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:922607582968:me-AmazonTextractASNS-11QV1D7FH37EF:8237a5f6-bac2-4fc2-a8f6-1dcedd6ee31f",
#                "MessageAttributes": {}
#            }
#        }
#    ]
#}