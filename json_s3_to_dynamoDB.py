import boto3
import json

###### When a json file is uploaded in s3 lambda function is triggered and copy the content to dynamodb table


s3_resource = boto3.resource('s3')
dynamo = boto3.resource('dynamodb')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name'] 
    file_name = event['Records'][0]['s3']['object']['key']

    json_object = s3_resource.Object(bucket_name,file_name)
    object_data = json_object.get()
    data = object_data['Body'].read()  # data as string
    jsondict = json.loads(data)        # data as dictionary
    
    tables = []
    for table in dynamo.tables.all():
        tables.append(table.table_name)
        
    print(tables)
    
    if file_name not in tables:            # if table not there, we create table same as the file name
        table = dynamo.create_table(
            TableName = file_name[:-5],
            KeySchema=[
                {
                    'AttributeName':'emp_id',
                    'KeyType':'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'emp_id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        
        table.wait_until_exists()
        
        print(f'Table status : CREATED "{table.table_name}" Table')
        
    table = dynamo.Table(file_name[:-5])
    print(table)
    
    response = table.put_item(Item=jsondict)
    
    return response