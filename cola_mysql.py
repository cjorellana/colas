import mysql.connector
import time

# Conexión a MySQL
cnx = mysql.connector.connect(user='root', password='password', host='127.0.0.1', database='my_database')
cursor = cnx.cursor()

# Añadir elementos a la cola no leída
data = "my data"
query = "INSERT INTO queue_table (data, status, processed_by, timestamp) VALUES (%s, %s, %s, %s)"
values = (data, 'unread', None, time.time())
cursor.execute(query, values)
cnx.commit()

# Procesar cada elemento no leído de la cola
while True:
    # Leer hasta 10 elementos de la cola
    query = "SELECT id, data FROM queue_table WHERE status = 'unread' LIMIT 10 FOR UPDATE"
    cursor.execute(query)
    items = cursor.fetchall()
    if items:
        for item in items:
            id, data = item

            # Aquí puedes procesar los datos del elemento de la forma que necesites

            # Registrar qué servidor procesó el elemento
            query = "UPDATE queue_table SET status = 'read', processed_by = %s WHERE id = %s"
            values = ("Server_1", id)  # Aquí debes obtener el nombre o ID de tu servidor de alguna manera
            cursor.execute(query, values)

        cnx.commit()
    
        
    # Comprobar si quedan elementos no leídos en la cola
    query = "SELECT COUNT(*) FROM queue_table WHERE status = 'unread'"
    cursor.execute(query)
    remaining = cursor.fetchone()[0]
    if remaining == 0:
        # Si no quedan elementos no leídos, esperar un poco antes de la próxima iteración
        time.sleep(60)

cursor.close()
cnx.close()