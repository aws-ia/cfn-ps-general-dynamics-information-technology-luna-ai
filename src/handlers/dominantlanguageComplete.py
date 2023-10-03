import json
import tarfile
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder
from io import BytesIO
from boto3.dynamodb.conditions import Key, Attr

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
sS3NlpLang = os.environ['S3NlpLang']

def LambdaHandler(event, context):
    srcBucket = ""
    srcPrefix = "'"
    srcKey = ""
    srcUri = ""
    destinBucket = sS3NlpLang
    destinPrefix = ""
    destinUri = ""
    sAssetId = ""

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
            print(srcbucketname)
            print(srckey)
            #print(srcPrefix)
            #print(sAssetId)
            #print(extension)
            #print(sJobId)
            #print("Job Status: " + json.dumps(detectJobState(sJobId), indent=4, cls=DateTimeEncoder))
        
            if extension == "gz": #unpack file to destination
                unpackTarFile(event, context, rec, srcbucketname, srckey, destinPrefix) #unpack
                deleteFile(srcbucketname, sAssetId + "/.write_access_check_file.temp") #delete temp file if it exist.
                deleteFile(srcbucketname, srckey) #delete old tar file
                objAssetDB = LoadAssetIdDB(sAssetId)
                sdominantlanguages = dominantlanguages(srcbucketname, sAssetId + "/" + "output")
                updateAssetLanguage(sAssetId, sdominantlanguages) #write the languages to dynamo.
                writeMessageEvent(event, objAssetDB, "comprehend", srckey.split("/")[1], "dominantlanguage", srcbucketname, "output", "", sdominantlanguages) #write to enichments

    return {
        'statusCode': 200,
        'body': json.dumps('Dominant Language Completion Event Recorded!')
    }

#check on job state
def detectJobState(sJobId):
    responseDetectJobStatus = comprehendClient.describe_dominant_language_detection_job(
        JobId = sJobId
        )
    return responseDetectJobStatus
    
#read the identified languages, which may be multiple. ISO standard.
def dominantlanguages(srcBucket, srcKey):
    objContent = s3Client.get_object(Bucket=srcBucket, Key=srcKey)
    jsonLanguages = json.loads(objContent['Body'].read())
    
    sLanguages = ""
    for l in jsonLanguages["Languages"]:
        sLanguages = sLanguages + l["LanguageCode"] + ","
    sLanguages = sLanguages[0:-1]
    print(sLanguages)
    return sLanguages

#unpack the comprehend files (wish this was not needed)
def unpackTarFile(event, context, rec, srcbucketname, srckey, destinPrefix):
    print("Source Bucket: " + srcbucketname + " Key: " + srckey)
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