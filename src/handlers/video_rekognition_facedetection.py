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
    
    response = detect(event, sSrcBucket, sSrcKey, sSrcExtension, event["detail"]["AssetId"], svid_rkfacedetectSNSarn, svid_rkfacedetectRolearn)

    return {
        'statusCode': 200,
        'body': json.dumps('Video Face Detection & Analysis Event Started!')
    }
        
    
#Detect video details Using AWS.
def detect(event, sBucket, sKey, sExt, sAssetId, sSNSTopicArn, sSNSRoleArn):
    response = clientRk.start_face_detection(
        Video={
            'S3Object': {
                'Bucket': sBucket,
                'Name': sKey
            }
        },
        NotificationChannel={
            'SNSTopicArn': sSNSTopicArn,
            'RoleArn': sSNSRoleArn
        },
        FaceAttributes='ALL',
        JobTag = sAssetId
    )
    return response