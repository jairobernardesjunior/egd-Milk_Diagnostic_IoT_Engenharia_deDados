import boto3
import botocore

def upload_s3(nome_buckets3, nome_arquivo, path_arquivo, access_key, secret_key, regiao):
    client = boto3.client(
        service_name= 's3',
        aws_access_key_id= access_key,
        aws_secret_access_key= secret_key,
        region_name= regiao # voce pode usar qualquer regiao
        ) 

    try:
        client.upload_file(path_arquivo, nome_buckets3, nome_arquivo)  
        return ''

    except botocore.exceptions.ClientError as e:
        # if e.response['Error']['Code'] == "404":
        return 'Erro - ' + str(e)