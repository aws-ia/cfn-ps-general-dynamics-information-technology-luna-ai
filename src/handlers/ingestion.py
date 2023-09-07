import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder
from decimal import Decimal

# S3txtract, S3txtractA, S3txtPlain, S3NlpTop, S3NlpNer, S3NlpLang, S3NlpKp, S3Kendra

#Pull in our clients.
clientS3 = boto3.client('s3')
resourceS3 = boto3.resource('s3')
clientEvents = boto3.client('events')
clientDoc = boto3.resource('dynamodb')
clientDB = boto3.client('dynamodb')

#Pull in environment variables
sAssetTableName = os.environ['AssetTable']
sAssetProcessingTableName = os.environ['AssetProcessingTable']
sAssetAttributeTableName = os.environ['AssetAttributeTable']
sAssetHistoryTable = os.environ['AssetHistoryTable']
sAssetFormatsTable = os.environ['AssetFormatsTable']
sAssetEnrichmentsTable = os.environ['AssetEnrichmentsTable']
sSearchTable = os.environ['SearchTable']
sSearchAggregateTable = os.environ['SearchAggregateTable']
sErrorTable = os.environ['ErrorTable']
sEventBusName = os.environ['EventBus_Name']
sTxtractBucket = os.environ['S3txtract']
sTxtractABucket = os.environ['S3txtractA']
sTxtPlainBucket = os.environ['S3txtPlain']
sTxtTopBucket = os.environ['S3NlpTop']
sTxtNerBucket = os.environ['S3NlpNer']
sTxtLangBucket = os.environ['S3NlpLang']
sTxtKpBucket = os.environ['S3NlpKp']
sTxtKendraBucket = os.environ['S3Kendra']
sS3Assets = os.environ['S3Assets']
sS3Translate = os.environ['S3Translate']
sS3Transcribe = os.environ['S3Transcribe']
sS3RkTxtDet = os.environ['S3RkTxtDet']
sS3RkSegDet = os.environ['S3RkSegDet']
sS3RkPplTrc = os.environ['S3RkPplTrc']
sS3RkLblDet = os.environ['S3RkLblDet']
sS3RkFacSrch = os.environ['S3RkFacSrch']
sS3RkFacDet = os.environ['S3RkFacDet']
sS3RkCeleb = os.environ['S3RkCeleb']
sS3RkMod = os.environ['S3RkMod']
sS3AzureDescrImg = os.environ['S3AzureDescrImg']
sIngestSQSurl = os.environ['ingestQueueUrl']

def LambdaHandler(event, context):
    #S3 event will be buried in body payload. So load message and then unpack body.

    for msg in event["Records"]:
        S3EventsNow = json.loads(msg["body"])
        
        for rec in S3EventsNow["Records"]:
            srcBucket = rec["s3"]["bucket"]["name"]
            srcKey = urllib.parse.unquote_plus(rec['s3']['object']['key'])
            srcExtension = srcKey.split(".")[-1]
            sAssetId = rec["s3"]["object"]["eTag"]
            destinDatabase = "demodb"
            assetBucket = sS3Assets
            assetPrefix = sAssetId + "/"
            assetKey = assetPrefix + sAssetId + "." + srcExtension
            
            #Process moving the asset into its final location
            responseAssetProcessing = processAsset(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase)
            
            #Write the DynamoDB Asset. Always do this after moving asset into the asset bucket.
            responseAssetAddition = writeAssetToDynamoDB(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase)
            
            #Write the event bridge message. Always do this after moving asset into the asset bucket.
            responseEventWrite = writeMessageEvent(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase)
 
# Not required if the Lambda is fired by SQS AND Finishes without throwing an error. If Lambda throws an error, the message is repeatedly sent
#until Lambda does not throw an error.
#           sqs.delete_message(
#                QueueUrl=sIngestSQSurl,
#                ReceiptHandle=receipt_handle
#                )
    
    return {
        'statusCode': 200,
        'body': json.dumps('S3 Ingestion Event Recorded!')
    }

#Obtain the etag from the head and use that as asset_id.
#NOTE - eTag is sometimes an MD5 hash, but not always. Later we should replace this with a MD5 or, ideally, a SHA 256 hash.
#Regardless, this allows us to now use an Asset ID and we can evolve it later.
def eHash(srcBucket, srcKey):
    responseS3 = clientS3.head_object(Bucket=srcBucket, Key=srcKey)
    strEtag = responseS3["ETag"].strip('"')
    return strEtag

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime)):
                return obj.isoformat()

#Write the ingestion event to the event bridge. It will kickoff the rest of the activities.
def writeMessageEvent(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase):
    appEvent = {
        "AssetId": sAssetId,
        "AssetDatabase": destinDatabase,
        "AssetSize": rec['s3']['object']['size'],
        "AssetType": srcExtension,
        "AssetStorage": "store", #store or analyze
        "AssetStorageBucket": assetBucket,
        "AssetStorageKey": assetKey,
        "AssetURL": "", #URL
        "AssetLanguage": "",
        "awsRegion": rec['awsRegion'],
        "srcEvent": rec['eventSource'],
        "srcEventTime": rec['eventTime'],
        "SrcBucket": srcBucket,
        "SrcKey": srcKey,
        "SrcExtension": srcExtension,
        "SrcIP": rec['requestParameters']['sourceIPAddress'],
        "SrcUser": "",
        "AssetCreationTime": str(int(time.time()))
    }
    
    bridgeEvent = {
        'EventBusName':sEventBusName,
        'Time': datetime.utcnow(),
        'Source':'gdit.me',
        'Resources' : [
            rec['s3']['bucket']['arn']
            ],
        'DetailType':'ingestion',
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
    
#Write the ingestion event to the event bridge. It will kickoff the rest of the activities.
def processAsset(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase):
    deleteAssetExistance(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase)
    copyContent(srcBucket, srcKey, assetBucket, assetKey) #copy ingestion file to long-term storage.
    deleteContent(srcBucket, srcKey) #delete the ingestion file, since we've now moved it to ingestion.

#Write the asset to the dynamodb table. Which also provides a stream of this content.
def writeAssetToDynamoDB(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase):
    table = clientDoc.Table(sAssetTableName)
    responseDynamoDB = table.put_item(
        Item={
            "AssetId": sAssetId,
            "AssetDatabase": destinDatabase,
            "AssetSize": rec['s3']['object']['size'],
            "AssetType": srcExtension,
            "AssetStorage": "store", #store or analyze
            "AssetStorageBucket": assetBucket,
            "AssetStorageKey": assetKey,
            "AssetURL": "", #URL
            "AssetLanguage": "",
            "srcEvent": rec['eventSource'],
            "srcEventTime": rec['eventTime'],
            "SrcBucket": srcBucket,
            "SrcKey": srcKey,
            "SrcExtension": srcExtension,
            "SrcIP": rec['requestParameters']['sourceIPAddress'],
            "SrcUser": "",
            "AssetCreationTime": str(int(time.time()))
        }
        )
    return responseDynamoDB

#If asset presently exists and is being re-imported. Remove all prior content.
#If the asset already exists, we assume it's being re-imported to analyze it using our new AI capabilities, ...
#so we delete all existing data FOR THAT ASSET only and perform the analysis fresh.
def deleteAssetExistance(rec, event, context, srcBucket, srcKey, srcExtension, sAssetId, assetBucket, assetKey, destinDatabase):
    deleteAssetFromDynamo(sAssetId) #clean up any records of this entry if it previously was ingested.
    deleteAssetFromS3(sAssetId)

#Delete a given asset from S3 if it presently exists.
#We'll do this if we want to actually delete the object or if we're reprocessing AI on said object.
def deleteAssetFromS3(sAssetId):
    deleteS3AssetID(sAssetId, sTxtractBucket)
    deleteS3AssetID(sAssetId, sTxtractABucket)
    deleteS3AssetID(sAssetId, sTxtPlainBucket)
    deleteS3AssetID(sAssetId, sTxtTopBucket)
    deleteS3AssetID(sAssetId, sTxtNerBucket)
    deleteS3AssetID(sAssetId, sTxtLangBucket)
    deleteS3AssetID(sAssetId, sTxtKpBucket)
    deleteS3AssetID(sAssetId, sTxtKendraBucket)
    deleteS3AssetID(sAssetId, sS3Assets)
    deleteS3AssetID(sAssetId, sS3Translate)
    deleteS3AssetID(sAssetId, sS3Transcribe)
    deleteS3AssetID(sAssetId, sS3RkTxtDet)
    deleteS3AssetID(sAssetId, sS3RkSegDet)
    deleteS3AssetID(sAssetId, sS3RkPplTrc)
    deleteS3AssetID(sAssetId, sS3RkLblDet)
    deleteS3AssetID(sAssetId, sS3RkFacSrch)
    deleteS3AssetID(sAssetId, sS3RkFacDet)
    deleteS3AssetID(sAssetId, sS3RkCeleb)
    deleteS3AssetID(sAssetId, sS3RkMod)
    deleteS3AssetID(sAssetId, sS3AzureDescrImg)
    deleteS3AssetID(sAssetId, sS3Assets)

#Deletes an asset from a given S3 bucket.
def deleteS3AssetID(sAssetId, sBucket):
    try:
        bucket = resourceS3.Bucket(sBucket)
        sPrefix = sAssetId
        
        #All S3 assets for a resource should be stored under the bucket under their given AssetID prefix.
        bucket.objects.filter(Prefix=sPrefix).delete()
    except Exception as e:
        print('Checking for Asset: ' + sAssetId + ' in bucket ' + sBucket + '. Object not existing is good. Information: ' + str(e))
    else:
        print('Deleted Asset: ' + sAssetId + ' from ' + sBucket + ' bucket.')

#Delete a given asset from DynamoDB if it presently exists.
#We'll do this if we want to actually delete the object or if we're reprocessing AI on said object.
def deleteAssetFromDynamo(sAssetId):
#    deleteDBAssetID(sAssetId, sErrorTable) #unused for now.
#    deleteDBAssetID(sAssetId, sAssetProcessingTableName) #unused for now.
#    deleteDBAssetID(sAssetId, sAssetHistoryTable) #unused for now.
    deleteDBFormats(sAssetId, sAssetFormatsTable)
#    deleteDBAssetID(sAssetId, sAssetAttributeTableName) #unused for now.
    deleteDBEnrichments(sAssetId, sAssetEnrichmentsTable)
    deleteDBSearch(sAssetId, sSearchTable)
    deleteDBSeachAgg(sAssetId, sSearchAggregateTable)
    deleteDBAssetID(sAssetId, sAssetTableName)


def deleteDBSearch(sAssetId, sTable):
    try:
        tableDynamoDB = clientDoc.Table(sTable)
        
        delItems = clientDB.query(
                TableName = sTable,
                IndexName = "AssetIdTerm",
                KeyConditionExpression = 'AssetId = :v1',
                ExpressionAttributeValues={
                    ':v1': {
                        'S': sAssetId,
                    }
                }
            )
            
        for delItem in delItems["Items"]:
            tableDynamoDB.delete_item(Key={'Term': delItem["Term"]["S"], 'Context': delItem["Context"]["S"]})

    except Exception as e:
        print('Checking for Asset: ' + sAssetId + ' in table ' + sTable + '. Object not existing is good. Information: ' + str(e))
    else:
        print('Deleted Asset: ' + sAssetId + ' from ' + sTable + ' table.')


def deleteDBSeachAgg(sAssetId, sTable):
    try:
        tableDynamoDB = clientDoc.Table(sTable)
        
        delItems = clientDB.query(
                TableName = sTable,
                ProjectionExpression= 'AssetId, ProcessType',
                KeyConditionExpression= 'AssetId = :v1',
                ExpressionAttributeValues={
                    ':v1': {
                        'S': sAssetId,
                    }
                }
            )
            
        for delItem in delItems["Items"]:
            tableDynamoDB.delete_item(Key={'AssetId': delItem["AssetId"]["S"], 'ProcessType': delItem["ProcessType"]["S"]})

    except Exception as e:
        print('Checking for Asset: ' + sAssetId + ' in table ' + sTable + '. Object not existing is good. Information: ' + str(e))
    else:
        print('Deleted Asset: ' + sAssetId + ' from ' + sTable + ' table.')

#Deletes AssetID from individual table
def deleteDBAssetID(sAssetId, sTable):
    try:
        tableDynamoDB = clientDoc.Table(sTable)
        tableDynamoDB.delete_item(Key={'AssetId': sAssetId})
    except Exception as e:
        print('Checking for Asset: ' + sAssetId + ' in table ' + sTable + '. Object not existing is good. Information: ' + str(e))
    else:
        print('Deleted Asset: ' + sAssetId + ' from ' + sTable + ' table.')
        
def deleteDBEnrichments(sAssetId, sTable):
    try:
        tableDynamoDB = clientDoc.Table(sTable)
        
        delItems = clientDB.query(
                TableName = sTable,
                ExpressionAttributeNames = {'#t': 'Timestamp'},
                ProjectionExpression= 'AssetId, #t',
                KeyConditionExpression= 'AssetId = :v1',
                ExpressionAttributeValues={
                    ':v1': {
                        'S': sAssetId,
                    }
                }
            )
            
        for delItem in delItems["Items"]:
            tableDynamoDB.delete_item(Key={'AssetId': delItem["AssetId"]["S"], 'Timestamp': Decimal(delItem["Timestamp"]["N"])})

    except Exception as e:
        print('Checking for Asset: ' + sAssetId + ' in table ' + sTable + '. Object not existing is good. Information: ' + str(e))
    else:
        print('Deleted Asset: ' + sAssetId + ' from ' + sTable + ' table.')

def deleteDBFormats(sAssetId, sTable):
    try:
        tableDynamoDB = clientDoc.Table(sTable)
        
        delItems = clientDB.query(
                TableName = sTable,
                ProjectionExpression= 'AssetId, FormatType',
                KeyConditionExpression= 'AssetId = :v1',
                ExpressionAttributeValues={
                    ':v1': {
                        'S': sAssetId,
                    }
                }
            )
            
        for delItem in delItems["Items"]:
            tableDynamoDB.delete_item(Key={'AssetId': delItem["AssetId"]["S"], 'FormatType': delItem["FormatType"]["S"]})

    except Exception as e:
        print('Checking for Asset: ' + sAssetId + ' in table ' + sTable + '. Object not existing is good. Information: ' + str(e))
    else:
        print('Deleted Asset: ' + sAssetId + ' from ' + sTable + ' table.')
    
#Copy stored assets from source to target.
def copyContent(strSrcBucket, strSrcKey, strDestBucket, strDestKey):
    copy_object={"Bucket":strSrcBucket,"Key":strSrcKey}
    
    print('Copy Object: ' + strSrcBucket + "  " + strSrcKey)
    
    try:
        #write copy statement 
        clientS3.copy_object(CopySource=copy_object,Bucket=strDestBucket,Key=strDestKey)
    except Exception as e:
        print('Information: ' + str(e))
    else:
        print('Copied: ' + strSrcBucket + ' Key: ' + strSrcKey + ' to Bucket: ' + strDestBucket + ' Key: ' + strDestKey)
    
def deleteContent(sBucket, sKey):
    try:
        resourceS3.meta.client.delete_object(Bucket=sBucket, Key=sKey)
    except Exception as e:
        print('Information: ' + str(e))
    else:
        print('Deleted Ingestion Item: Bucket: ' + sBucket + ' Key: ' + sKey)