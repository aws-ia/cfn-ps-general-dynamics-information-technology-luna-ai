import tarfile
from io import BytesIO
import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

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
sS3NlpNer = os.environ['S3NlpNer']
#sS3Assets = os.environ['S3Assets']
sComprehendAccessarn = os.environ['ComprehendAccessarn']
sConfidenceScore = os.environ['rkConfidence']
sSearchTable = os.environ['SearchTable']
sSearchAggregateTable = os.environ['SearchAggregateTable']

def LambdaHandler(event, context):
    print("Incoming Event")
    print(json.dumps(event))

    for rec in event["Records"]:
        payload = json.loads(rec["body"])
        for rec2 in payload["Records"]:
            #print(json.dumps(rec2))
            srcbucketname = rec2["s3"]["bucket"]["name"]
            srckey = rec2["s3"]["object"]["key"]
            srcPrefix = srckey.split("/")[0] + "/" + srckey.split("/")[1]
            sAssetId = srckey.split("/")[0]
            #assetid 
            destinPrefix = sAssetId
            extension =  srckey.split(".")[-1] #tar.gz, so this should return .gz
            sJobId = srckey.split("/")[-3].split("-")[-1]
            
            if extension == "gz": #unpack file to destination
                unpackTarFile(event, context, rec, srcbucketname, srckey, destinPrefix) #unpack
                deleteFile(srcbucketname, sAssetId + "/.write_access_check_file.temp") #delete temp file if it exist.
                deleteFile(srcbucketname, srckey) #delete old tar file
                objAssetDB = LoadAssetIdDB(sAssetId)
                print("SourceBucket2: " + srcbucketname + " Key: " + sAssetId + "/" + "output")
                enrichments = enrichmentsGen(srcbucketname, sAssetId + "/" + "output", "aws-comprehend", sAssetId)
                #writeMessageEvent(event, objAssetDB, "comprehend", sJobId, "ner", srcbucketname, "output", "", sdominantlanguages) #write to enichments
                enrichmentEvent(event, objAssetDB, enrichments, "comprehend", sJobId, "ner", srcbucketname, "output", "")

    return {
        'statusCode': 200,
        'body': json.dumps('Dominant Language Completion Event Recorded!')
    }

#check on job state
def detectJobState(sJobId):
    responseDetectJobStatus = comprehendClient.describe_entities_detection_job(
        JobId = sJobId
        )
    return responseDetectJobStatus
    
#read the identified languages, which may be multiple. ISO standard.
def enrichmentsGen(srcBucket, srcKey, sProcessType, sAssetId):
    objContent = s3Client.get_object(Bucket=srcBucket, Key=srcKey)
    jsonEnrichments = json.loads(objContent['Body'].read())
    sAggregate = ""
    iCount = 0
    
    enrichments = []
    for ent in jsonEnrichments["Entities"]:
        sText = ent["Type"].title() + ": " + ent["Text"].title()
        
        if ent["Type"] not in("OTHER", "QUANTITY") and (float(ent["Score"])*100) > float(sConfidenceScore):
            writeSearchEnrichmentDynamoDB(sProcessType, sAssetId, sText, (float(ent["Score"])*100))
            
            #Limit records in the aggregate to high-quality
            if iCount<30:
                if ent["Type"] in("PERSON", "ORGANIZATION") and (float(ent["Score"])*100) > float(95):
                    sAggregate = sAggregate + sText + "; "
                    iCount = iCount + 1 #increment count
        
            if {"Type": ent["Type"].title(), "Text": ent["Text"].title()} not in enrichments:
                enrichments.append({"Type": ent["Type"].title(), "Text": ent["Text"].title(), "Score": ent["Score"]})

    data = {}
    data["Enrichments"]=enrichments
    writeSearchEnrichmentAggregateDynamoDB(sProcessType, sAssetId, sAggregate, "", srcBucket, srcKey)
    
    return data

#unpack the comprehend files (wish this was not needed)
def unpackTarFile(event, context, rec, srcbucketname, srckey, destinPrefix):
    print("Source Bucket1: " + srcbucketname + " Key: " + srckey)
    input_tar_file = s3Client.get_object(Bucket = srcbucketname, Key = srckey)
    input_tar_content = input_tar_file['Body'].read()    
    with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if (tar_resource.isfile()):
                inner_file_bytes = tar.extractfile(tar_resource).read()
                #print("Destin Bucket: " + srcbucketname + " Key: " + destinPrefix + "/" + tar_resource.name)
                s3Client.upload_fileobj(BytesIO(inner_file_bytes), Bucket = srcbucketname, Key = destinPrefix + "/" + tar_resource.name)
                

#delete old files, if they exist
def deleteFile(bucket, deletekey):
    try:
        key = deletekey
        print(json.dumps('Delete ' + bucket + " " + key))
        s3Client.delete_object(Bucket=bucket, Key=key)
    except (ValueError, TypeError):
        print(json.dumps('Error File!'))

#set the language in the asset table
def updateAssetLanguage(sAssetId, sLanguages):
    table = dbResource.Table(sAssetTableName)

    response = table.update_item(
        Key={
            'AssetId': sAssetId
        },
        UpdateExpression="set AssetLanguage=:lang",
        ExpressionAttributeValues={
            ':lang': sLanguages
        },
        ReturnValues="UPDATED_NEW"
    )
    return response

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
    
#Write the event to the event bridge. It will kickoff other.
def enrichmentEvent(event, objAssetDB, enrichments, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension):
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
#    print(json.dumps(bridgeEvent, indent=4, cls=DateTimeEncoder))
    print("event me")
#    print(json.dumps(responseEventPublish))
    print(json.dumps(bridgeEvent, indent=4, cls=DateTimeEncoder))
    return responseEventPublish #allow caller to determine whether to use response id or not.
    

def writeSearchEnrichmentDynamoDB(sProcessType, sAssetId, sEnrichment, nConfidence):
    table = dbResource.Table(sSearchTable)
    
    dConfidence = round(Decimal(nConfidence), 4)
    
    responseDynamoDB = table.put_item(
        Item={
            "Term": sEnrichment,
            "Context": sProcessType + "-" + sAssetId,
            "Confidence": dConfidence,
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