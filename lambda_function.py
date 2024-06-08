import json
import random
import string
import boto3

def generate_data():
   return{
    "bookingId": str(random.randint(1000,9999)),
    "userId": str(random.randint(1000,9999)),
    "propertyId": str(random.randint(1000,9999)),
    "location": random.choice(["California","Newyork","Washington","Berlin"]),
    "startDate":random.choice(["2024-03-12","2024-03-13","2024-03-14"]),
    "endDate":random.choice(["2024-03-13","2024-03-14","2024-03-15"]),
    "price":'$ ' + str(random.randint(100,999))
    }   

def lambda_handler(event, context):
    sqs_client=boto3.client('sqs')
    print("-------------Sending Data to SQS------------------")  
    for i in range(10):
        message=generate_data()
        print(json.dumps(message))
              
        sqs_client.send_message(QueueUrl ='https://sqs.us-east-1.amazonaws.com/950907486899/messgQ',MessageBody=json.dumps(message))
        print("Data sent to SQS")    
            # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
        
    }
