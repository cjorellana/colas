import mysql.connector
import time
import requests

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
    # Leer un solo elemento de la cola
    query = "SELECT id, data FROM queue_table WHERE status = 'unread' LIMIT 1 FOR UPDATE"
    cursor.execute(query)
    item = cursor.fetchone()
    if item:
        id, data = item
        try:
            # Aquí puedes procesar los datos del elemento de la forma que necesites
            api_url = "https://jsonplaceholder.typicode.com/todos/1"
            response = requests.get(api_url)
            if response.status_code == 200:
                # Registrar qué servidor procesó el elemento
                query = "UPDATE queue_table SET status = 'read', processed_by = %s WHERE id = %s"
                values = ("Server_1", id)  # Aquí debes obtener el nombre o ID de tu servidor de alguna manera
                cursor.execute(query, values)
                cnx.commit()
            else:
                raise Exception("API call failed")
        except Exception as e:
            print(f"Error processing item {id}: {e}")
            cnx.rollback()

    # Comprobar si quedan elementos no leídos en la cola
    query = "SELECT COUNT(*) FROM queue_table WHERE status = 'unread'"
    cursor.execute(query)
    remaining = cursor.fetchone()[0]
    if remaining == 0:
        # Si no quedan elementos no leídos, esperar un poco antes de la próxima iteración
        time.sleep(60)

cursor.close()

cnx.close()