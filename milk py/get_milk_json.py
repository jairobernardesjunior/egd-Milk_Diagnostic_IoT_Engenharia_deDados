# https://youtu.be/K21BSZPFIjQ
"""
Extraia e-mails selecionados da sua conta do Gmail
1. Certifique-se de ativar o IMAP nas configurações do Gmail
(Faça login na sua conta do Gmail e vá para Configurações, Ver todas as configurações e selecione
 Guia Encaminhamento e POP/IMAP. Na seção "Acesso IMAP", selecione Ativar IMAP.)
2. Se você tiver autenticação de dois fatores, o Gmail exige que você crie um aplicativo
senha específica que você precisa usar.
Vá para as configurações da sua conta do Google e clique em 'Segurança'.
Role para baixo até App Passwords em verificação em duas etapas.
Selecione Mail em Selecionar aplicativo. e Outro em Selecionar dispositivo. (Dê um nome, por exemplo, python)
O sistema fornece uma senha que você precisa usar para autenticar no python.
"""

import imaplib
import email
import yaml 
import datetime
import pandas as pd
from fc import fc_upload_s3 as ups3

def grava_sobes3_arquivo_json_lapidado(
        dirAux, nome_arquivo, df, s3_dados_processed, 
        s3_dados_processed_permanente, access_key, secret_key, regiao,
        url_imapx, portax, remetentex, passwx, destinatariox):

    try:
        nome_arquivo = nome_arquivo + '.json'
        pathJson = dirAux + '/' + nome_arquivo

        df.to_json(pathJson)
        retorno = True

    except Exception as Error:
        retorno = Error
        print(retorno) 
        texto=retorno
        subject="Erro ao gravar arquivo na pasta -> " + nome_arquivo
        email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)             
        exit()         

    """

    # ******************** CARREGA ARQUIVO json NO BUCKET S3
    retorno = ups3.upload_s3(
            s3_dados_processed, nome_arquivo, pathJson, access_key, secret_key, regiao)

    if retorno != True:
        print(retorno)
        texto = 'bucket s3 => ' + s3_dados_processed + ' arquivo => ' + nome_arquivo + 
                ' --- ' + retorno + ' ***** não foi carregado'   

    retorno = ups3.upload_s3(
            s3_dados_processed_permanente, nome_arquivo, pathJson, access_key, secret_key, regiao)

    if retorno != True:
        print(retorno)
        texto = 'bucket s3 => ' + s3_dados_processed + ' arquivo => ' + nome_arquivo + 
                ' --- ' + retorno + ' ***** não foi carregado'
        subject="Erro ao carregar o arquivo no buckets3 -> " + s3_dados_processed
        email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject) 

    """
    return retorno    

def verifica_nro(campo):
    i=0
    while i<len(campo):
        if campo[i] not in ['-','.','0','1','2','3','4','5','6','7','8','9']:
            if i==0:
                c1=''
            else:
                c1=campo[0:i-1]

            ii=i+1          
            if ii<len(campo):
                c2=campo[ii:len(campo)-1]
            else:
                c2=''

            campo=c1 + '0' + c2

        i+=1

    print(campo)

    return campo

def lambda_handler(event, context):
# ******************** INÍCIO

# parâmetros em arquivo de credenciais
    with open("credentials.yml") as f:
        content = f.read()
        
    my_credentials = yaml.load(content, Loader=yaml.FullLoader)

    # separa parâmetros em suas variáveis
    user, password = my_credentials["user"], my_credentials["password"]
    access_key, secret_key = my_credentials["access_key"], my_credentials["secret_key"]
    region, s3_dados_processed = my_credentials["region"], my_credentials["mk-s3-milk-json"]
    s3_dados_processed_permanente = my_credentials["mk-s3-milk-json-permanente"]
    imap_url, dirAux = my_credentials["url_imap"], my_credentials["dirAux"]
    url_imapx, portax = my_credentials["url_imap"], my_credentials["porta"]
    remetentex, passwx = my_credentials["user"], my_credentials["password"]
    destinatariox = my_credentials["destinatario"]     

    # conecta gmail
    my_mail = imaplib.IMAP4_SSL(imap_url)

    # loga usuário
    my_mail.login(user, password)

    # Seleciona as mensagens
    my_mail.select('Inbox')

    # filtra email com chave e valor
    key = 'FROM'
    value = 'i4x.data@gmail.com'
    _, data = my_mail.search(None, key, value)  #Search for emails with specific key and value
    status, search_data = my_mail.search(None, 'ALL')

    mail_id_list = data[0].split()
    msgs = []

    # carrega msgs
    for num in mail_id_list:
        typ, data = my_mail.fetch(num, '(RFC822)') #RFC822 returns whole message (BODY fetches just body)
        msgs.append(data)

    # Separa campos do email e monta documento json
    cod_produtor=[]
    datax=[]
    horax=[]
    lat=[]
    long=[]
    umidade=[]
    tempex=[]
    tleite1=[]
    tleite2=[]
    tleite3=[]
    tleite4=[]
    tleite5=[]
    tleite6=[]
    tleite7=[]  
    tleite8=[] 

    cod_produtor.clear
    datax.clear
    horax.clear
    lat.clear
    long.clear
    umidade.clear
    tempex.clear
    tleite1.clear
    tleite2.clear
    tleite3.clear
    tleite4.clear
    tleite5.clear
    tleite6.clear
    tleite7.clear
    tleite8.clear     

    ii=0
    for msg in msgs[::-1]:
        for response_part in msg:
            if type(response_part) is tuple:
                my_msg=email.message_from_bytes((response_part[1]))
                #print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
                #print ("subj:", my_msg['subject'])
                #print ("from:", my_msg['from'])   

                for part in my_msg.walk():  
                    if part.get_content_type() == 'text/plain':
                        body = part.get_payload()[0:3000]
                        body = body.replace('\r\n', '')
                        body_aux=body
                        pos=body_aux.find('r1p')
                        body_aux= body[pos:3000]

                        print(body_aux)
                        #print(body_aux[0:2])
                        #print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")     
                               
                        if body_aux[0:2] == 'r1':                             
                            i=0
                            while i<15:
                                pos=body_aux.find('p') + 1
                                body_aux= body_aux[pos:3000]

                                pos=body_aux.find('p')
                                campo= body_aux[0:pos]

                                #print(campo)
                                if i>2:
                                    campo=verifica_nro(campo)

                                match i:
                                    case 0:                               
                                        cod_produtor.append(campo)
                                    case 1:
                                        datax.append(campo)
                                    case 2:
                                        horax.append(campo)
                                    case 3:
                                        lat.append(campo)
                                    case 4:
                                        long.append(campo)
                                    case 5:
                                        umidade.append(campo)
                                    case 6:
                                        tempex.append(campo)
                                    case 7:
                                        tleite1.append(campo)
                                    case 8:
                                        tleite2.append(campo)
                                    case 9:
                                        tleite3.append(campo)
                                    case 10:
                                        tleite4.append(campo)
                                    case 11:
                                        tleite5.append(campo)
                                    case 12:
                                        tleite6.append(campo)
                                    case 13:
                                        tleite7.append(campo)
                                    case 14:
                                        tleite8.append(campo)

                                i+=1 
                                ii+=1  

    if ii>0:
        df=pd.DataFrame({
                "cod_produtor":cod_produtor,
                "datax":datax,
                "horax":horax,
                "lat":lat,
                "long":long,
                "umidade":umidade,
                "tempex":tempex,
                "tleite1":tleite1,
                "tleite2":tleite2,
                "tleite3":tleite3,
                "tleite4":tleite4,
                "tleite5":tleite5,
                "tleite6":tleite6,
                "tleite7":tleite7,
                "tleite8":tleite8,                         
                })

        nome_arquivo = 'milk_' + str(datetime.datetime.now())
        nome_arquivo = nome_arquivo.replace(' ', '_')
        nome_arquivo = nome_arquivo.replace(':', '')
        nome_arquivo = nome_arquivo.replace('.', '_')
        retorno = grava_sobes3_arquivo_json_lapidado(
                    dirAux, nome_arquivo, df, s3_dados_processed, s3_dados_processed_permanente,
                    access_key, secret_key, region,
                    url_imapx, portax, remetentex, passwx, destinatariox)       

        if retorno == True:
            mail_ids = []

            for block in search_data:
                mail_ids += block.split()

            # definindo o range da operação
            start = mail_ids[0].decode()
            end = mail_ids[-1].decode()

            # movendo os emails para a lixeira
            # este passo é específico do gmail
            # que não permite a exclusão direta
            my_mail.store(f'{start}:{end}'.encode(), '+X-GM-LABELS', '\\Trash')                    

    else:
        print("+++++ Email sem dados")
        print (my_msg) 
        texto=my_msg
        subject="+++++ Email sem dados -> " + nome_arquivo
        email.envia_email(url_imapx, portax, remetentex, passwx, destinatariox, texto, subject)             
        exit()                         

lambda_handler(1, 1)                            