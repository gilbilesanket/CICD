import json
import boto3
import pandas as pd

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
#sns_arn = 'arn:aws:sns:us-east-1:851725469799:s3-arrival-notification'

def lambda_handler(event, context):
    result=[]
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    # Read the JSON file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    #raw_data=response['Body'].read()
    #print(raw_data)
    json_data = response['Body'].read().decode('utf-8')
    
    # Convert JSON data to pandas DataFrame
    #df = pd.DataFrame(json_data)
    jsonobj=json_data.split('\r\n')
    # Process the DataFrame as needed
    for obj in jsonobj:
        if not obj.strip():
            continue
        else:
            jsondict=json.loads(obj)
            result.append(jsondict) 
    
    df=pd.DataFrame(result)
    print(df[df['status']=="delivered"])
    
    message="Data filtered with status as delivered and sent to S3"
    print("publising message to sns_client")
    try:
    # Publish message to SNS topic
        sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:950907486899:fromlambdatosns',
        Message=message,
        Subject="Notification"
        )
    except Exception as e:
        print(f"Error publishing message to SNS topic: {e}")

    #sns_client.publish(TopicArn='arn:aws:sns:us-east-1:950907486899:fromlambdatosns:0ef87e37-83ca-4307-8803-dee82119e224',
    #Message=message,Subject="Notification")

    print("done !!! message sent to the subscriber")
    print(message)    
    
    return {
        'statusCode': 200,
        'body': json.dumps('JSON file read successfully')
    }




