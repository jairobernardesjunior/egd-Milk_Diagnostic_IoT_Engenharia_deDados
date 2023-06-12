import yaml 
import pandas as pd
from fc import fc_download_s3 as dows3
from fc import fc_delete_s3 as dels3
from fc import fc_list_objects_buckets3 as lists3
from fc import fc_conecta_db as conect
from fc import fc_envia_email as email
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
    hostx, databasex = my_credentials["host"], my_credentials["database"]
    userdb, passwdb = my_credentials["user_db"], my_credentials["password_db"]
    url_imapx, portax = my_credentials["url_imap"], my_credentials["porta"]
    remetentex, passwx = my_credentials["user"], my_credentials["password"]
    destinatariox = my_credentials["destinatario"]    

    # connecta banco de dados
    retorno = conect.conecta_db(hostx, databasex, userdb, passwdb)
    if retorno[0][0:5] == 'erro':
        print(retorno[0])
        texto=retorno[0]
        subject="Erro ao conectar banco de dados -> " + databasex
        email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)             
        exit()    

    # le lista de arquivos buckets3
    retorno = lists3.list_objects_buckets3(s3_dados_processed, secret_key, access_key) 
    if retorno[0][0:5] == 'erro':
        print(retorno[0])
        texto=retorno[0]
        subject="Erro ao listar objetos do buckets3 -> " + s3_dados_processed
        email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)             
        exit()         
        
    linhas= retorno.count
    if len(linhas) == 0:
        print('não foi encontrado nenhum .json no bucket s3')
        texto=retorno[0]
        subject="Não foi encontrado nenhum .json no bucket s3 -> " + s3_dados_processed
        email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)             
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
        
        if not retorno:
            print(retorno) 
            texto=retorno
            subject="Erro ao baixar arquivo do buckets3 -> " + s3_dados_processed + " - " + nome_arquivo
            email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)             
            exit()        

        try:
            df = pd.read_json(path_arquivo, encoding='utf-8-sig')
            engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/db_bix_teste')
            df.to_sql('milk', engine, if_exists='replace', index=False)
        except Exception as error:
            print(error)
            texto=error
            subject="Erro ao gravar no banco de dados -> " + databasex
            email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)             
            exit() 

        retorno = dels3.delete_s3(s3_dados_processed, 
                                    nome_arquivo, 
                                    access_key, 
                                    secret_key, 
                                    region)     

        if not retorno:
            print(retorno)
            texto=retorno
            subject="Erro ao deletar arquivo no buckets3 -> " + s3_dados_processed + " - " + nome_arquivo
            email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)        
        
        i+=1               

lambda_handler(1, 1)                            