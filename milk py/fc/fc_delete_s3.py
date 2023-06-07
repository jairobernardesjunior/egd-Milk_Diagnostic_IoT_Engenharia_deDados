import boto3
from botocore.exceptions import ClientError

def delete_s3(nome_buckets3, keyname, access_key, secret_key, regiao):
    client = boto3.client(
        service_name= 's3',
        aws_access_key_id= access_key,
        aws_secret_access_key= secret_key,
        region_name= regiao # voce pode usar qualquer regiao
        ) 

    retorno = False

    try:
        s3_client = boto3.client('s3')
        response = s3_client.delete_object(Bucket=nome_buckets3,Key=keyname)
        retorno = True
        
    except ClientError as e:
        retorno = e.response['Error']   

    return retorno