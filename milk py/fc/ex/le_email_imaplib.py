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

def lambda_handler(event, context):
# ******************** INÍCIO

# parâmetros em arquivo de credenciais
    with open("credentials.yml") as f:
        content = f.read()
        
    my_credentials = yaml.load(content, Loader=yaml.FullLoader)

    # separa parâmetros em suas variáveis
    user, password = my_credentials["user"], my_credentials["password"]
    imap_url = my_credentials["url_imap"]

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

    # carrega msgs
    for num in mail_id_list:
        typ, data = my_mail.fetch(num, '(RFC822)') #RFC822 returns whole message (BODY fetches just body)
        msgs.append(data)

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

                        #print(body_aux)
                        #print(body_aux[0:2])
                        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")                     

                        if body_aux[0:2] == 'r1':                            
                            i=0
                            while i<15:
                                pos=body_aux.find('p') + 1
                                body_aux= body_aux[pos:3000]

                                pos=body_aux.find('p')
                                campo= body_aux[0:pos]

                                print(campo)

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

                        mail_ids = []

                        for block in search_data:
                            mail_ids += block.split()

                        # definindo o range da operação
                        start = mail_ids[0].decode()
                        end = mail_ids[-1].decode()

                        # movendo os emails para a lixeira
                        # este passo é específico do gmail
                        # que não permite a exclusão direta
                        #my_mail.store(f'{start}:{end}'.encode(), '+X-GM-LABELS', '\\Trash')                                   

lambda_handler(1, 1)                            