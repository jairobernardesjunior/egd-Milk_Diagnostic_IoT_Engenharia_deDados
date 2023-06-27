import boto3
#import pprint
from botocore.exceptions import ClientError

def list_objects_buckets3(s3_dados_processed, secret_key, access_key):
    df=[]

    try:
        session = boto3.Session( aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        s3 = session.resource('s3')
        my_bucket = s3.Bucket(s3_dados_processed)

        for my_bucket_object in my_bucket.objects.all():
            df.append(my_bucket_object.key)        

    except ClientError as e:
        #print(e)
        df.append('Erro - ' + e.response['Error'])
        
    return df