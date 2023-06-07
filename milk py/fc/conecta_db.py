# Faz a conexão com o banco de dados passando parâmetros
import psycopg2

def conecta_db(hostx, databasex, userx, passwordx):
  con = psycopg2.connect(host= hostx, 
                         database= databasex,
                         user= userx, 
                         password= passwordx)
  return con