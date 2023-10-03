import json
import boto3
import os
import urllib.parse
from datetime import datetime
import time
import botocore
from json import JSONEncoder

s3Client = boto3.client('s3')
clientEvents = boto3.client('events')
clientRk = boto3.client('rekognition') 

sAssetTableName = os.environ['AssetTable']
sAssetFormatsTableName = os.environ['AssetFormatsTable']
sErrorTable = os.environ['ErrorTable']
sEventBusName = os.environ['EventBus_Name']
sS3RkFacDet = os.environ['S3RkFacDet']
sS3Assets = os.environ['S3Assets']
srkConfidence = os.environ['rkConfidence']
svid_rkfacedetectSNSarn = os.environ['vid_rkfacedetectSNSarn']
svid_rkfacedetectRolearn = os.environ['vid_rkfacedetectRolearn'] 

def LambdaHandler(event, context):
    print(json.dumps(event))
    print(context)

    #Loop over the records here and send them for execution by proper set.
    for rec in event["Records"]:
        message = json.loads(rec["Sns"]["Message"])


        if message["Status"] == "SUCCEEDED":
            response = rkFaceDetection(message, event, context)
            
            AssetId = message["JobTag"]
            AssetType = "StaticVideo"
            AssetStorage = "store"
            AssetStorageBucket = sS3Assets
            AssetStorageKey = message["Video"]["S3ObjectName"].split("/")[1]
            AssetURL = "" #assuming static storage. Videos at URLs will be downloaded.
            AssetLanguage = "" #language is unknown at this point.
            ComponentSource = "vidfacedetect_awsrk"
            ProcessId = message["JobId"]
            ProcessType = "static_vid"
            ProcessOutputBucket = sS3RkFacDet
            ProcessOutputKey = message["JobTag"] + "/" + message["JobTag"] + ".json"
            ProcessExtension = "json"

            persistDetectiontoS3(event, response, message["JobId"], sS3RkFacDet, ProcessOutputKey)
            
            #Note: This event generation generates 1 event per detected face, so a video will have a stream of face detections w timestamps.
            faceEnrichmentEvent(event, message, response, AssetId, AssetType, AssetStorage, AssetStorageBucket, AssetStorageKey, AssetURL, AssetLanguage, ComponentSource, ProcessId, ProcessType, ProcessOutputBucket, ProcessOutputKey, ProcessExtension, sEventBusName)
        else:
            print("Error")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed Video Face Detection Records!')
    }
    
def rkFaceDetection(rec, event, context):
    response = clientRk.get_face_detection(
        JobId=rec["JobId"],
        MaxResults=1000
    )
    combinedresponse = response
    
    while "NextToken" in response: #if token is in the response, it's paginated, so we need to loop over pages.
        response = clientRk.get_face_detection(
            JobId=rec["JobId"],
            MaxResults=1000,
            NextToken= response["NextToken"]
        )
        #combinedresponse["Faces"].push(response["Faces"]) #Add faces from each page to the response.
    
    return combinedresponse
    
    
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
def faceEnrichmentEvent(sEvent, sMessage, sEnrichments, sAssetId, sAssetType, sAssetStorage, sAssetStorageBucket, sAssetStorageKey, sAssetURL, sAssetLanguage, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension, sEventBus):
    #Fire incoming video event.
    appEvent(sEvent, sMessage, sEnrichments["VideoMetadata"], sAssetId, sAssetType, sAssetStorage, sAssetStorageBucket, sAssetStorageKey, sAssetURL, sAssetLanguage, sComponentSource, sProcessId, sProcessType + "_vidmetadata", sProcessOutputBucket, sProcessOutputKey, sProcessExtension, sEventBus, sComponentSource)

    i = 0 #face counter    
    #Stream each face.
    for face in sEnrichments["Faces"]:
        appEvent(sEvent, sMessage, face, sAssetId, sAssetType, sAssetStorage, sAssetStorageBucket, sAssetStorageKey, sAssetURL, sAssetLanguage, sComponentSource, sProcessId, sProcessType + "_vidfacedetect", sProcessOutputBucket, sProcessOutputKey, sProcessExtension, sEventBus, sComponentSource)
        i = i+1
    
    print("Faces Processed: " + str(i))
        
def appEvent(sEvent, sMessage, sEnrichments, sAssetId, sAssetType, sAssetStorage, sAssetStorageBucket, sAssetStorageKey, sAssetURL, sAssetLanguage, sComponentSource, sProcessId, sProcessType, sProcessOutputBucket, sProcessOutputKey, sProcessExtension, sEventBus, sDetailType):
    #appEvent(AssetId, AssetDatabase, AssetSize, AssetType, AssetStorage, AssetStorageBucket, AssetURL, AssetLanguage, ComponentSource, 
    #ProcessType, ProcessId, ProcessOutputBucket, ProcessOutputKey, ProcessExtension, ProcessData, ProcessStartTime, Enrichments, 
    #BusName, Time, Source, DetailType, Detail)
    
    appEvent = {
        "AssetId": sAssetId,
        "AssetType": sAssetType,
        "AssetStorage": sAssetStorage, #store or analyze
        "AssetStorageBucket": sAssetStorageBucket,
        "AssetStorageKey": sAssetStorageKey,
        "AssetURL": sAssetURL, #URL
        "AssetLanguage": sAssetLanguage,
        "ComponentSource": sComponentSource,
        "ProcessType": sProcessType,
        "ProcessId": sProcessId,
        "ProcessOutputBucket": sProcessOutputBucket,
        "ProcessOutputKey": sProcessOutputKey,
        "ProcessExtension": sProcessExtension,
        "ProcessData": "",
        "ProcessStartTime": str(int(time.time())),
        "Enrichments": sEnrichments
    }
    
    bridgeEvent = {
        'EventBusName':sEventBus,
        'Time': datetime.utcnow(),
        'Source':'gdit.me',
        'Resources' : [],
        'DetailType': sDetailType,
        'Detail': json.dumps(appEvent, indent=4, cls=DateTimeEncoder)
    }
    
    # Send event to EventBridge
    responseEventPublish = clientEvents.put_events(
        Entries=[
            bridgeEvent
            ]
    )

#    print(json.dumps(bridgeEvent, indent=4, cls=DateTimeEncoder))
    return responseEventPublish #allow caller to determine whether to use response id or not.