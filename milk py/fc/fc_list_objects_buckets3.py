import boto3
#import pprint
from botocore.exceptions import ClientError

def list_objects_buckets3(bucket_name, secret_key, access_key):
    df=[]
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        s3_client = boto3.resource('s3')
        bucket = s3_client.Bucket(bucket_name)
                # Para cada objeto encontrado, vamos mostraro nome/keyname
        for obj in bucket.objects.all():
            #pprint.pprint(obj.key) #Usamos o pprint para uma saída mais amigável do resultado.
            df.append(obj.key)

    except ClientError as e:
        #print(e)
        df.append('erro ' + e.response['Error'])
        
    return df