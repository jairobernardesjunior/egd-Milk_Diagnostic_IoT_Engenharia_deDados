import boto3
import yaml

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

df=[]

session = boto3.Session( aws_access_key_id=access_key, aws_secret_access_key=secret_key)

s3 = session.resource('s3')

my_bucket = s3.Bucket(s3_dados_processed)

print(str(my_bucket))

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key)
    df.append(my_bucket_object.key)

print(df)