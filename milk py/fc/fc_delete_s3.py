import boto3
from botocore.exceptions import ClientError

def delete_s3(nome_buckets3, keyname, access_key, secret_key, regiao):
    client = boto3.client(
        service_name= 's3',
        aws_access_key_id= access_key,
        aws_secret_access_key= secret_key,
        region_name= regiao # voce pode usar qualquer regiao
        ) 

    try:
        client.delete_object(Bucket=nome_buckets3, Key=keyname)
        return ''
        
    except ClientError as e:
        return 'Erro - ' + str(e)