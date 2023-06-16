import unittest
from cola_redis import process_queue  # Aquí importas la función process_queue desde el archivo cola.py

class TestProcessQueue(unittest.TestCase):

    def setUp(self):
        # Conexión a Redis
        self.r = redis.Redis(host='localhost', port=6379, db=0)

    def test_process_queue_empty(self):
        # Comprueba que la función regresa True si la cola está vacía
        self.r.delete('unread-queue')  # Asegura que la cola esté vacía
        self.assertTrue(process_queue(self.r))

    def test_process_queue_non_empty(self):
        # Comprueba que la función regresa False si la cola no está vacía
        self.r.delete('unread-queue')  # Asegura que la cola esté vacía
        data = {'data': 'my data', 'processed_by': None, 'timestamp': time.time()}
        self.r.rpush('unread-queue', json.dumps(data))
        self.assertFalse(process_queue(self.r))

if __name__ == '__main__':
    unittest.main()