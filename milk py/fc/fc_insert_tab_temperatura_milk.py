import psycopg2

def insert_tab_temperatura_milk(conn,                 
                                cod_produtor,
                                datax,
                                horax,
                                lat,
                                long,
                                umidade,
                                tempex,
                                tleite1,
                                tleite2,
                                tleite3,
                                tleite4,
                                tleite5,
                                tleite6,
                                tleite7,  
                                tleite8                 
                                ):

    cursor = conn.cursor()

    try:        
        postgres_insert_query = """ INSERT INTO tab_temperatura_milk (cod_produtor,
                                                                        datax,
                                                                        horax,
                                                                        lat,
                                                                        long,
                                                                        umidade,
                                                                        tempex,
                                                                        tleite1,
                                                                        tleite2,
                                                                        tleite3,
                                                                        tleite4,
                                                                        tleite5,
                                                                        tleite6,
                                                                        tleite7,  
                                                                        tleite8)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        
        record_to_insert = (cod_produtor,
                            datax,
                            horax,
                            lat,
                            long,
                            umidade,
                            tempex,
                            tleite1,
                            tleite2,
                            tleite3,
                            tleite4,
                            tleite5,
                            tleite6,
                            tleite7, 
                            tleite8)          
            
        cursor.execute(postgres_insert_query, record_to_insert)
        conn.commit()
        retorno = ''
        
    except (Exception, psycopg2.Error) as error:       
        retorno = 'Erro - ' + str(error)

    cursor.close
    return retorno