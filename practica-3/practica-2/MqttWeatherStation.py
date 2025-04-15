# Este programa simula una estación meteorológica utilizando MQTT. 
# Existen dos modos de operación: publisher (publicador) y subscriber (suscriptor). 
# El publicador genera datos simulados de sensores y los envía al broker MQTT. 
# El suscriptor escucha esos datos, los procesa y los almacena en una base de datos MySQL.

# Para crear la base de datos:

# CREATE DATABASE estacion_meteorologica;
# USE estacion_meteorologica;

# Crear la tabla de sensores: sensor_data:

# CREATE TABLE sensor_data (
#    id INT AUTO_INCREMENT PRIMARY KEY,
#    sensor_id VARCHAR(50) NOT NULL,
#    temperature DECIMAL(5, 2) NOT NULL,
#    humidity DECIMAL(5, 2) NOT NULL,
#    pressure DECIMAL(6, 2) NOT NULL,
#    wind_speed DECIMAL(5, 2) NOT NULL,
#    tiestamp DATETIME NOT NULL
# );

# Se debe cambiar el la contrasena del DB en caso de que se elija una diferente a

# DB_PASSWORD = "12345678"


# Para ejecutar el programa se debe hacer en el siguiente orden:

# 1 modo publicador
# python MqttWeatherStation.py --mode publisher --stations station1 station2

# 2 modo subscritor
# python MqttWeatherStation.py --mode subscriber --stations station1 station2

import paho.mqtt.client as mqtt
import mysql.connector
import random
import time
import json
import argparse
from threading import Thread


# Configuration for MQTT Broker
BROKER = "test.mosquitto.org"
PORT = 1883
KEEPALIVE = 60
PUBLISH_INTERVAL = 2  # Intervalo entre publicaciones en segundos

# Configuration for MySQL Database
DB_HOST = "localhost"
DB_USER = "root"  # Cambia esto según tu usuario de MySQL
DB_PASSWORD = "12345678"
DB_NAME = "estacion_meteorologica"

# Conexión a MySQL
def connect_db():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return connection
    except mysql.connector.Error as err:
        print(f"[Database] Error conectando a MySQL: {err}")
        return None

def insert_sensor_data(sensor_id, temperature, humidity, pressure, wind_speed, timestamp):
    connection = connect_db()
    if connection:
        cursor = connection.cursor()
        query = """
        INSERT INTO sensor_data (sensor_id, temperature, humidity, pressure, wind_speed, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (sensor_id, temperature, humidity, pressure, wind_speed, timestamp)
        cursor.execute(query, values)
        connection.commit()
        cursor.close()
        connection.close()
        print("[Database] Data inserted successfully.")
    else:
        print("[Database] No se pudo insertar datos.")

class Sensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id

    def generate_data(self):
        return {
            "sensor_id": self.sensor_id,  # Cambiado de "sensorId" a "sensor_id"
            "temperature": round(random.uniform(-10, 40), 2),
            "humidity": round(random.uniform(10, 90), 2),
            "pressure": round(random.uniform(950, 1050), 2),
            "wind_speed": round(random.uniform(0, 20), 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

class Publisher:
    def __init__(self, client_id):
        print(f"Initializing Publisher with client_id: {client_id}")
        self.client = mqtt.Client(client_id, protocol=mqtt.MQTTv311)
        self.client_id = client_id
        self.connected = False

    def connect(self):
        try:
            print(f"[{self.client_id}] Connecting to broker {BROKER}:{PORT}")
            self.client.connect(BROKER, PORT, KEEPALIVE)
            self.client.loop_start()  # Start a background thread to handle MQTT communication
            self.connected = True
            print(f"[{self.client_id}] Successfully connected to broker.")
        except Exception as e:
            self.connected = False
            print(f"[{self.client_id}] Error connecting to broker: {e}")

    def disconnect(self):
        try:
            if self.connected:
                print(f"[{self.client_id}] Disconnecting from broker")
                self.client.loop_stop()  # Stop the background thread
                self.client.disconnect()
        except Exception as e:
            print(f"[{self.client_id}] Error disconnecting from broker: {e}")

    def send_message(self, topic, message):
        try:
            if not self.connected:
                print(f"[{self.client_id}] Not connected to broker. Attempting to reconnect...")
                self.connect()

            print(f"[{self.client_id}] Publishing message to topic {topic}")
            result = self.client.publish(topic, json.dumps(message), qos=1, retain=False)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"[{self.client_id}] Message successfully sent: {message}")
            else:
                print(f"[{self.client_id}] Failed to send message with result code: {result.rc}")
        except Exception as e:
            print(f"[{self.client_id}] Error publishing message: {e}")

    def start_simulation(self, topic):
        print(f"Starting indefinite simulation for topic: {topic}")
        sensor = Sensor(self.client_id)
        try:
            self.connect()
            while True:
                data = sensor.generate_data()
                print(f"Generated data: {data}")
                self.send_message(topic, data)
                time.sleep(PUBLISH_INTERVAL)
        except KeyboardInterrupt:
            print("[Publisher] Simulation stopped by user.")
        except Exception as e:
            print(f"[Publisher] Simulation error: {e}")
        finally:
            self.disconnect()

class Subscriber:
    def __init__(self, client_id):
        print(f"Initializing Subscriber with client_id: {client_id}")
        self.client = mqtt.Client(client_id, protocol=mqtt.MQTTv311)
        self.is_connected = False

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            if not self.is_connected:
                print("[Subscriber] Successfully connected to MQTT Broker.")
                self.is_connected = True
        else:
            print(f"[Subscriber] Connection failed with code {rc}")

    def on_message(self, client, userdata, message):
        try:
            raw_message = message.payload.decode()
            print(f"[Subscriber] Received raw message from topic {message.topic}: {raw_message}")
            try:
                payload = json.loads(raw_message)
                print(f"[Subscriber] Decoded JSON message: {payload}")

                # Cambiar "sensorId" por "sensor_id" antes de guardar en MySQL
                if "sensorId" in payload:
                    payload["sensor_id"] = payload.pop("sensorId")

                insert_sensor_data(**payload)  # Guarda en MySQL
            except json.JSONDecodeError:
                print(f"[Subscriber] Received non-JSON message: {raw_message}")
        except Exception as e:
            print(f"[Subscriber] Error processing message: {e}")

    def start_listening(self, topic):
        print(f"[Subscriber] Connecting to broker {BROKER}:{PORT} and subscribing to topic {topic}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        try:
            self.client.connect(BROKER, PORT, 60)
            print(f"[Subscriber] Subscribed to topic: {topic}")
            self.client.subscribe(topic, qos=2)

            while True:
                self.client.loop(timeout=1.0)
        except KeyboardInterrupt:
            print("[Subscriber] Listening stopped by user.")
        finally:
            self.disconnect()

    def disconnect(self):
        if self.is_connected:
            try:
                print("[Subscriber] Disconnecting from broker.")
                self.client.disconnect()
                self.is_connected = False
            except Exception as e:
                print(f"[Subscriber] Error during disconnect: {e}")

def start_multiple_stations(station_ids):
    threads = []
    for station_id in station_ids:
        topic = f"/estacion/{station_id}/sensores"
        publisher = Publisher(station_id)
        thread = Thread(target=publisher.start_simulation, args=(topic,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT Weather Station Simulator")
    parser.add_argument("--mode", choices=["publisher", "subscriber"], required=True, help="Run as publisher or subscriber")
    parser.add_argument("--stations", nargs="*", default=["station1"], help="Station IDs to simulate (for publisher)")
    parser.add_argument("--topic", default="/estacion/+/sensores", help="MQTT topic to subscribe (for subscriber)")
    args = parser.parse_args()

    if args.mode == "publisher":
        start_multiple_stations(args.stations)
    elif args.mode == "subscriber":
        subscriber = Subscriber("subscriber-1")
        subscriber.start_listening(args.topic)