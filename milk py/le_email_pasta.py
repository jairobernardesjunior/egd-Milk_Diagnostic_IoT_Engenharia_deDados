import email, imaplib
import yaml 

# parâmetros em arquivo de credenciais
with open("credentials.yml") as f:
    content = f.read()
    
my_credentials = yaml.load(content, Loader=yaml.FullLoader)

# separa parâmetros em suas variáveis
user, password = my_credentials["user"], my_credentials["password"]
imap_url = my_credentials["url_imap"]

host= imap_url

username= user
password= password

mail= imaplib.IMAP4_SSL(host)
mail.login(username, password)

print(mail.list())