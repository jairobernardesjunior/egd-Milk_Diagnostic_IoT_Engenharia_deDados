import psycopg2
  
def create_table(cursor):
    try:
        cursor.execute("select exists(select * from information_schema.tables \
                       where table_name=tab_temperatura_milk)", ('mytable',))        
        cursor.fetchone()[0]

        cursor.rollback()
        cursor.close()        
        return ''

    except Exception as error:  
        erro=str(error)
        pos=erro.find('tab_temperatura_milk" does not exist')
        if pos <= 0:  
            cursor.rollback()
            cursor.close()             
            return 'Erro - ' + erro

    """ create tables in the PostgreSQL database"""
    command = (
        """
        CREATE TABLE tab_temperatura_milk (
            cod_produtor integer,
            datax date,
            horax time without time zone,
            lat double precision,
            "long" double precision,
            umidade double precision,
            tempex double precision,
            tleite1 double precision,
            tleite2 double precision,
            tleite3 double precision,
            tleite4 double precision,
            tleite5 double precision,
            tleite6 double precision,
            tleite7 double precision,
            tleite8 double precision 
            )
        """)

    try:
        cursor.execute(command)
        cursor.commit()
        cursor.close()  
        return ''     

    except (Exception, psycopg2.DatabaseError) as error:
        cursor.rollback()
        cursor.close()
        return 'Erro - ' + str(error)