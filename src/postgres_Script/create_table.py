import psycopg2
from postgres_Connection import postgres_conn

# Database connection settings
conn= postgres_conn()
cur = conn.cursor()


# SQL to create a table
create_table_sql = """
CREATE TABLE IF NOT EXISTS stg_SensorInfo (
    record JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_SensorInfo_json (
    record JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS SensorInfo (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    sensor_id INTEGER,
    value DOUBLE PRECISION,
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Execute the SQL command
cur.execute(create_table_sql)

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Table created successfully.")