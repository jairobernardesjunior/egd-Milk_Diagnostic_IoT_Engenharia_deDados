from imap_tools import MailBox, AND
import yaml

# parâmetros em arquivo de credenciais
with open("credentials.yml") as f:
    content = f.read()
    
my_credentials = yaml.load(content, Loader=yaml.FullLoader)

# separa parâmetros em suas variáveis
userx, passwordx = my_credentials["user"], my_credentials["password"]

# pegar emails de um remetente para um destinatário
username = userx
password = passwordx

# lista de imaps: https://www.systoolsgroup.com/imap/
meu_email = MailBox('imap.gmail.com').login(username, password)

# criterios: https://github.com/ikvk/imap_tools#search-criteria
lista_emails = meu_email.fetch(criteria=AND(from_="jairobernardesjunior@gmail.com")) 
for email in lista_emails:
    print(email.subject)
    print(email.text)

# pegar emails com um anexo específico
lista_emails = meu_email.fetch(criteria=AND(from_="jairobernardesjunior@gmail.com")) 
for email in lista_emails:
    print(email.subject)
    print(email.text)