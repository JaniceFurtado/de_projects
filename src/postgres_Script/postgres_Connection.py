import psycopg2

def postgres_conn():
    # Database connection settings
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="de_proj",
        user="postgres",
        password="postgres"
    )
    
    return conn