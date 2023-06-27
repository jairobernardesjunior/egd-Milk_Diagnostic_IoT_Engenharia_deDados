import yaml 
import pandas as pd
from datetime import datetime
from fc import fc_download_s3 as dows3
from fc import fc_delete_s3 as dels3
from fc import fc_list_objects_buckets3 as lists3
from fc import fc_conecta_db as conect
from fc import fc_envia_email as email
from fc import fc_insert_tab_temperatura_milk as insere

def corrige_hora(df):
    df['datax'] = df.datax.str.replace(' ', '0')
    df['horax'] = df.horax.str.replace(' ', '0')
    df['horax'] = df.horax.str.replace('253', '21')
    df['horax'] = df.horax.str.replace('254', '22')
    df['horax'] = df.horax.str.replace('255', '23')

    print(df)

    return df

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
    porta_dbx, tab_temperatura_milkx = my_credentials["porta_db"], my_credentials["tab_temperatura_milk"]
    userdb, passwdb = my_credentials["user_db"], my_credentials["password_db"]
    url_imapx, portax = my_credentials["url_imap"], my_credentials["porta"]
    remetentex, password_remetentex = my_credentials["remetente"], my_credentials["password_remetente"]
    destinatariox = my_credentials["destinatario"]    

    # connecta banco de dados
    conn = conect.conecta_db(hostx, databasex, userdb, passwdb)
    retorno=str(conn)

    if retorno[0:4] == 'Erro':
        texto=retorno
        data_e_hora_atuais = datetime.now()
        data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')        
        subject="Erro ao conectar banco de dados -> " + databasex + " em " + data_e_hora_em_texto
        email.envia_email(url_imapx, portax, remetentex, password_remetentex, destinatariox, texto, subject)             
        exit()    

    # le lista de arquivos buckets3
    lista = lists3.list_objects_buckets3(s3_dados_processed, secret_key, access_key) 

    if not lista:
        texto='Não existe arquivos .json no bucket s3 -> ' + s3_dados_processed
        data_e_hora_atuais = datetime.now()
        data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')         
        subject='Não existe arquivos .json no bucket s3 -> ' + s3_dados_processed + " em " + data_e_hora_em_texto
        email.envia_email(url_imapx, portax, remetentex, password_remetentex, destinatariox, texto, subject)             
        exit()

    if lista[0][0:4] == 'Erro':
        texto=str(lista)
        data_e_hora_atuais = datetime.now()
        data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')         
        subject="Erro ao listar objetos do buckets3 -> " + s3_dados_processed + " em " + data_e_hora_em_texto
        email.envia_email(url_imapx, portax, remetentex, password_remetentex, destinatariox, texto, subject)             
        exit()         
        
    linhas= len(lista)
    if linhas == 0:
        texto='Não foi encontrado nenhum .json no bucket s3 - ' + retorno
        data_e_hora_atuais = datetime.now()
        data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')         
        subject="Não foi encontrado nenhum .json no bucket s3 -> " + s3_dados_processed + " em " + data_e_hora_em_texto
        email.envia_email(url_imapx, portax, remetentex, password_remetentex, destinatariox, texto, subject)             
        exit()         

    i=0
    print(dirAux)

    # varre lista de arquivos .json baixados do buckets3
    while i<linhas:
        nome_arquivo=lista[i]
        path_arquivo=dirAux + '/' + nome_arquivo

        # baixa arquivos json do buckets3
        retorno = dows3.download_s3(s3_dados_processed, 
                                    nome_arquivo, 
                                    path_arquivo, 
                                    access_key, 
                                    secret_key, 
                                    region)
        
        retorno=str(retorno)
        
        if retorno[0:4] == 'Erro':
            print(retorno) 
            texto=retorno
            data_e_hora_atuais = datetime.now()
            data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')             
            subject="Erro ao baixar arquivo do buckets3 -> " + s3_dados_processed + " - " + nome_arquivo + " em " + data_e_hora_em_texto
            email.envia_email(url_imapx, portax, remetentex, password_remetentex, destinatariox, texto, subject)             
            exit()    

        df = pd.read_json(path_arquivo, encoding='utf-8-sig')
        df = corrige_hora(df)     

        linhas_df= len(df.index)
        ii=0

        # varre linhas do arquivo .json para inserir dados no db milk_diagnostic
        while ii<linhas_df:
            retorno= insere.insert_tab_temperatura_milk(
                                        conn,                 
                                        int(df.iloc[ii, 0]),#cod_produtor
                                        df.iloc[ii, 1],#datax
                                        df.iloc[ii, 2],#horax
                                        df.iloc[ii, 3],#lat
                                        df.iloc[ii, 4],#long
                                        df.iloc[ii, 5],#umidade
                                        df.iloc[ii, 6],#tempex
                                        df.iloc[ii, 7],#tleite1
                                        df.iloc[ii, 8],#tleite2
                                        df.iloc[ii, 9],#tleite3
                                        df.iloc[ii, 10],#tleite4
                                        df.iloc[ii, 11],#tleite5
                                        df.iloc[ii, 12],#tleite6
                                        df.iloc[ii, 13],#tleite7
                                        df.iloc[ii, 14]#tleite8                
                                        )
            
            retorno=str(retorno)
        
            if retorno[0:4] == 'Erro':
                texto=retorno
                data_e_hora_atuais = datetime.now()
                data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')        
                subject="Erro ao inserir linhas na tab_temperatura_milk -> " + \
                        databasex + " em " + data_e_hora_em_texto
                email.envia_email(url_imapx, portax, remetentex, password_remetentex, destinatariox, texto, subject)             

            ii+=1

        # apaga o arquivo .json do buckets3, inserido no db milk_diagnostic
        retorno = dels3.delete_s3(s3_dados_processed, 
                                    nome_arquivo, 
                                    access_key, 
                                    secret_key, 
                                    region)     

        retorno=str(retorno)
        
        if retorno[0:4] == 'Erro':
            print(retorno)
            texto=retorno
            data_e_hora_atuais = datetime.now()
            data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')             
            subject="Erro ao deletar arquivo no buckets3 -> " + s3_dados_processed + " - " + \
                    nome_arquivo + " em " + data_e_hora_em_texto
            email.envia_email(url_imapx, portax, remetentex, password_remetentex, destinatariox, texto, subject)        
        
        i+=1    

    conn.close           

lambda_handler(1, 1)                            