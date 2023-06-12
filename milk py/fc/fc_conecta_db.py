# Faz a conexão com o banco de dados passando parâmetros
import psycopg2

def conecta_db(hostx, databasex, userx, passwordx):
  retorno=False

  try:
    con = psycopg2.connect(host= hostx, 
                          database= databasex,
                          user= userx, 
                          password= passwordx)
    retorno=con
  except Exception as error:
    retorno= 'Erro - ' + error

  return retorno