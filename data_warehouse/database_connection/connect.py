import psycopg2
from config import load_config_localDWH, load_config_localDB, load_config_prodDWH, load_config_prodDB

def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.' + conn.dsn)
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)



if __name__ == '__main__':
    config = load_config_prodDWH()
    # config = load_config_prodDB()
    # config = load_config2()
    # config = load_config()
    connect(config)
    print(config)
    # test()