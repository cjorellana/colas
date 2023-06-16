import redis
import json
import time
import traceback

def process_queue2():
    pass

def process_queue():
    # Conexión a Redis
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Procesar cada elemento no leído de la cola
    while True:
        try:
            # Leer hasta 10 elementos de la cola
            items = r.lrange('unread-queue', 0, 9)

            if items:
                for item in items:
                    # Deserializar los datos del elemento
                    item_data = json.loads(item)

                    # Aquí puedes procesar los datos del elemento de la forma que necesites

                    # Registrar qué servidor procesó el elemento
                    item_data['processed_by'] = "Server_1"  # Aquí debes obtener el nombre o ID de tu servidor de alguna manera

                    # Añadir el elemento a la cola leída
                    r.rpush('read-queue', json.dumps(item_data))

                # Eliminar los elementos procesados de la cola no leída
                r.ltrim('unread-queue', len(items), -1)

            if r.llen('unread-queue') == 0:
                # Si no hay elementos no leídos, esperar un poco antes de la próxima iteración
                time.sleep(30)

        except redis.exceptions.ConnectionError as e:
            print("Error de conexión con Redis. Reintentando en 30 segundos.")
            print(traceback.format_exc())  # Imprime la pila de llamadas del error
            time.sleep(30)

        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}")
            print(traceback.format_exc())  # Imprime la pila de llamadas del error
            # Aquí podrías decidir qué hacer en caso de un error al decodificar el JSON.
            # Por ejemplo, podrías decidir eliminar el elemento malformado de la cola.

        except Exception as e:
            print(f"Error inesperado: {e}")
            print(traceback.format_exc())  # Imprime la pila de llamadas del error
            # Aquí podrías decidir qué hacer en caso de un error inesperado.
            # Por ejemplo, podrías decidir detener el programa, o tal vez quieras esperar un poco y luego volver a intentarlo.

if __name__ == '__main__':
    process_queue()