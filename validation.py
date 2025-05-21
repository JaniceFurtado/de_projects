#VALIDATION

from postgres_Connection import postgres_conn

conn = postgres_conn()
cur= conn.cursor()
cur.execute(
            "SELECT * FROM SensorInfo;"
        )
result = cur.fetchall()
# Print the results to validate the insertion
print("Validation Result:")
for row in result:
    print(row)
    
cur.close()
conn.close()