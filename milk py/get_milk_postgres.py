import yaml 
import pandas as pd
from fc import fc_download_s3 as dows3
from fc import fc_delete_s3 as dels3
from fc import fc_list_objects_buckets3 as lists3
from sqlalchemy import create_engine

def lambda_handler(event, context):
# ******************** INÍCIO
# parâmetros em arquivo de credenciais
    with open("credentials.yml") as f:
        content = f.read()
        
    my_credentials = yaml.load(content, Loader=yaml.FullLoader)

    # separa parâmetros em suas variáveis
    access_key, secret_key = my_credentials["access_key"], my_credentials["secret_key"]
    region, s3_dados_processed = my_credentials["region"], my_credentials["mk-s3-milk-json"]
    dirAux = my_credentials["dirAux"]

    # le lista de arquivos buckets3
    retorno = lists3.list_objects_buckets3(s3_dados_processed, secret_key, access_key) 
    if retorno[0][0:6] == 'false':
        print(retorno[0])
        exit()
        
    linhas= retorno.count
    if len(linhas) == 0:
        print('não foi encontrado nenhum .json no bucket s3')
        exit()

    i=0
    while i<linhas:
        nome_arquivo=retorno[i, 0]
        path_arquivo=dirAux + '/' + nome_arquivo

        # baixa arquivos json do buckets3
        retorno = dows3.download_s3(s3_dados_processed, 
                                    nome_arquivo, 
                                    path_arquivo, 
                                    access_key, 
                                    secret_key, 
                                    region)
        
        if retorno: 
            df = pd.read_json(path_arquivo, encoding='utf-8-sig')
            engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/db_bix_teste')
            df.to_sql('milk', engine, if_exists='replace', index=False)

        retorno = dels3.delete_s3(s3_dados_processed, 
                                    nome_arquivo, 
                                    access_key, 
                                    secret_key, 
                                    region)            
        
        i+=1               

lambda_handler(1, 1)                            