import paho.mqtt.client as mqtt
import mysql.connector
import random
import time
import json
import argparse
from threading import Thread
from datetime import datetime
from flask import Flask, jsonify, request
import requests

# 🔐 Constantes para API externa
EXTERNAL_API_URL = "https://itt363-hub.smar.com.do/api/"
EXTERNAL_API_TOKEN = "Q2rGSGkCy4bR"  # Token del Grupo #4

# Flask app initialization
app = Flask(__name__)

# MQTT Broker
BROKER = "test.mosquitto.org"
PORT = 1883
KEEPALIVE = 60
PUBLISH_INTERVAL = 2  # segundos

# 🔁 Función para enviar a API externa
def enviar_a_api_externa(sensor_id, temperatura, humedad, timestamp):
    try:
        fecha_convertida = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")

        payload = {
            "grupo": "4",
            "estacion": str(sensor_id),
            "fecha": fecha_convertida,
            "temperatura": float(temperatura),
            "humedad": float(humedad)
        }

        headers = {
            "SEGURIDAD-TOKEN": EXTERNAL_API_TOKEN,
            "Content-Type": "application/json"
        }

        response = requests.post(EXTERNAL_API_URL, headers=headers, json=payload)

        if response.status_code == 200:
            print(f"[API EXTERNA] ✅ Datos enviados correctamente: {payload}")
        else:
            print(f"[API EXTERNA] ❌ Error {response.status_code}: {response.text}")
    except Exception as e:
            print(f"[API EXTERNA] ❌ Excepción al enviar datos: {e}")


# ✅ Conexión a tu servidor Linux
def get_db_connection():
    return mysql.connector.connect(
        host="192.168.100.169",
        user="Cristian0517",
        password="Noviembre0824@",
        database="estacion_meteorologica"
    )

# Función para registrar logs en la base de datos
def log_event(level, message, **extra):
    """
    Registra un evento en la tabla de logs
    
    Args:
        level: Nivel del log (INFO, ERROR, WARNING)
        message: Mensaje descriptivo
        **extra: Datos adicionales (pasados como keywords)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convertir datos extra a formato JSON
        extra_data = json.dumps(extra) if extra else None
        
        # Obtener timestamp actual
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # SQL para insertar en la tabla de logs
        query = """
        INSERT INTO log_events (level, message, extra_data, timestamp)
        VALUES (%s, %s, %s, %s)
        """
        values = (level, message, extra_data, timestamp)
        
        cursor.execute(query, values)
        conn.commit()
        
        # Imprimir en consola para debug
        print(f"[LOG] [{level}] {message} | {extra}")
        
    except mysql.connector.Error as e:
        print(f"[LOG] ❌ Error al registrar log: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def insert_sensor_data(sensor_id, temperature, humidity, pressure, wind_speed, timestamp):
    print(f"[DB] Insertando desde {sensor_id} a las {timestamp}")
    table_map = {
        "station1": "sensor_data",
        "station2": "sensor_data2",
        "station3": "sensor_data3",
        "station4": "sensor_data4",
    }
    table = table_map.get(sensor_id, "sensor_data")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"""
        INSERT INTO {table} (sensor_id, temperature, humidity, pressure, wind_speed, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (sensor_id, temperature, humidity, pressure, wind_speed, timestamp)
        cursor.execute(query, values)
        conn.commit()
        
        # Registrar log de inserción exitosa
        log_event("INFO", f"Datos insertados para {sensor_id}", 
                table=table, 
                temperature=temperature, 
                humidity=humidity, 
                pressure=pressure, 
                wind_speed=wind_speed)
                
        print(f"[DB] ✅ Insertado correctamente en {table}")
    except mysql.connector.Error as e:
        # Registrar log de error
        log_event("ERROR", f"Error al insertar datos desde {sensor_id}", 
                error=str(e), 
                table=table)
        print(f"[DB] ❌ Error al insertar: {e}")
    finally:
        if conn: conn.close()

# Sensor personalizado por estación
class Sensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id
        self.config = self.get_sensor_config(sensor_id)

    def get_sensor_config(self, sensor_id):
        return {
            "station1": {"temp": (-5, 10), "hum": (20, 40), "pres": (970, 990), "wind": (5, 15)},
            "station2": {"temp": (20, 35), "hum": (50, 80), "pres": (1000, 1020), "wind": (2, 8)},
            "station3": {"temp": (15, 25), "hum": (30, 60), "pres": (980, 1010), "wind": (1, 5)},
            "station4": {"temp": (30, 45), "hum": (10, 30), "pres": (990, 1005), "wind": (10, 20)},
        }.get(sensor_id, {"temp": (0, 30), "hum": (30, 60), "pres": (990, 1010), "wind": (0, 10)})

    def generate_data(self):
        return {
            "sensor_id": self.sensor_id,
            "temperature": round(random.uniform(*self.config["temp"]), 2),
            "humidity": round(random.uniform(*self.config["hum"]), 2),
            "pressure": round(random.uniform(*self.config["pres"]), 2),
            "wind_speed": round(random.uniform(*self.config["wind"]), 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

# Publisher
class Publisher:
    def __init__(self, client_id):
        self.client = mqtt.Client(client_id, protocol=mqtt.MQTTv311)
        self.client_id = client_id
        self.connected = False

    def connect(self):
        try:
            self.client.connect(BROKER, PORT, KEEPALIVE)
            self.client.loop_start()
            self.connected = True
            log_event("INFO", f"Conectado al broker MQTT", client_id=self.client_id)
        except Exception as e:
            log_event("ERROR", f"Error al conectar MQTT", client_id=self.client_id, error=str(e))
            print(f"[{self.client_id}] ❌ Error al conectar MQTT: {e}")

    def disconnect(self):
        if self.connected:
            self.client.loop_stop()
            self.client.disconnect()
            log_event("INFO", f"Desconectado del broker MQTT", client_id=self.client_id)

    def send_message(self, topic, message):
        if not self.connected:
            self.connect()
        self.client.publish(topic, json.dumps(message), qos=1)

    def start_simulation(self, topic):
        sensor = Sensor(self.client_id)
        self.connect()
        try:
            while True:
                data = sensor.generate_data()
                print(f"[{self.client_id}] Enviando: {data}")
                self.send_message(topic, data)
                time.sleep(PUBLISH_INTERVAL)
        except KeyboardInterrupt:
            log_event("INFO", f"Simulación detenida por el usuario", client_id=self.client_id)
            print(f"[{self.client_id}] ✋ Detenido por el usuario.")
        finally:
            self.disconnect()

class Subscriber:
    def __init__(self, client_id):
        self.client = mqtt.Client(client_id, protocol=mqtt.MQTTv311)
        self.client_id = client_id

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log_event("INFO", "Subscriber conectado al broker MQTT", client_id=self.client_id)
            print("[Subscriber] ✅ Conectado al broker")
            self.client.subscribe("meteorologia/+/sensores")
            print("[Subscriber] 🔔 Suscrito a meteorologia/+/sensores")
        else:
            log_event("ERROR", "Error al conectar subscriber", client_id=self.client_id, error_code=rc)
            print(f"[Subscriber] ❌ Error: {rc}")

    def on_message(self, client, userdata, message):
        try:
            raw = message.payload.decode()
            print(f"[Subscriber] 📩 Recibido mensaje en {message.topic}: {raw}")
            payload = json.loads(raw)

            try:
                timestamp = datetime.strptime(payload.get("time"), "%y/%m/%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"[Subscriber] ⚠️ Error al parsear fecha: {e}")
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            sensor_data = {
                "sensor_id": payload.get("station_id"),
                "temperature": payload.get("temperature"),
                "humidity": payload.get("humidity"),
                "pressure": payload.get("atmospheric_pressure"),
                "wind_speed": payload.get("wind_speed"),
                "timestamp": timestamp
            }

            print(f"[HTTP] 🌐 Enviando a servidor: {sensor_data}")
            response = requests.post("https://itt363-4.eict.ce.pucmm.edu.do/insert-lora", json=sensor_data)
            print(f"[HTTP] ✅ Respuesta: {response.status_code} - {response.text}")

            log_event("INFO", "Datos recibidos por MQTT procesados correctamente",
                      topic=message.topic,
                      sensor_id=payload.get("station_id"))

            # ✅ Enviar también a la API externa (opcional)
            enviar_a_api_externa(
                sensor_id=sensor_data["sensor_id"],
                temperatura=sensor_data["temperature"],
                humedad=sensor_data["humidity"],
                timestamp=sensor_data["timestamp"]
            )

        except Exception as e:
            log_event("ERROR", f"Error procesando mensaje MQTT", error=str(e))
            print(f"[Subscriber] ❌ Error procesando mensaje MQTT: {e}")

    def start_listening(self, topic):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        try:
            print(f"[Subscriber] 🔄 Conectando a {BROKER}:{PORT}")
            self.client.connect(BROKER, PORT, KEEPALIVE)

            topic_to_use = "meteorologia/+/sensores"
            log_event("INFO", f"Subscriber iniciado", topic=topic_to_use, client_id=self.client_id)
            print(f"[Subscriber] 🎧 Iniciando escucha en {topic_to_use}")

            self.client.subscribe(topic_to_use)
            self.client.loop_forever()
        except Exception as e:
            log_event("ERROR", f"Error al conectar subscriber", error=str(e))
            print(f"[Subscriber] ❌ Error de conexión: {e}")




# Ejecutar múltiples estaciones
def start_multiple_stations(station_ids):
    threads = []
    for sid in station_ids:
        topic = f"/estacion/{sid}/sensores"
        publisher = Publisher(sid)
        thread = Thread(target=publisher.start_simulation, args=(topic,))
        threads.append(thread)
        thread.start()
        log_event("INFO", f"Estación iniciada", station_id=sid, topic=topic)
    for thread in threads:
        thread.join()

# API Route para obtener logs
@app.route("/logs", methods=["GET"])
def obtener_logs():
    try:
        # Parámetros opcionales para paginación
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 100))
        offset = (page - 1) * limit
        
        # Filtro opcional por nivel
        level_filter = request.args.get('level')
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Construir la consulta base
        query = "SELECT * FROM log_events"
        params = []
        
        # Añadir filtro de nivel si se especificó
        if level_filter:
            query += " WHERE level = %s"
            params.append(level_filter)
            
        # Añadir ordenamiento y límites
        query += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        logs = cursor.fetchall()
        
        # Contar errores para el contador
        cursor.execute("SELECT COUNT(*) as count FROM log_events WHERE level = 'ERROR'")
        error_count = cursor.fetchone()['count']
        
        # Formatear los logs para la respuesta
        formatted_logs = []
        for log in logs:
            log_entry = {
                "level": log['level'],
                "message": log['message'],
                "timestamp": log['timestamp'].strftime("%Y-%m-%d %H:%M:%S") if isinstance(log['timestamp'], datetime) else log['timestamp']
            }
            
            # Añadir datos extra si existen
            if log['extra_data']:
                try:
                    extra_data = json.loads(log['extra_data'])
                    log_entry.update(extra_data)
                except:
                    log_entry['extra_data'] = log['extra_data']
                    
            formatted_logs.append(log_entry)
            
        cursor.close()
        conn.close()
        
        return jsonify({"logs": formatted_logs, "errores": error_count}), 200
        
    except Exception as e:
        # Registrar el error (no en la BD para evitar recursión si hay error de BD)
        print(f"[API] ❌ Error obteniendo logs: {e}")
        return jsonify({"status": "fail", "message": f"Error al obtener logs: {e}", "logs": []}), 500

# Ruta para obtener datos de sensores
@app.route("/sensores", methods=["GET"])
def obtener_sensores():
    try:
        # Parámetros opcionales
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
        offset = (page - 1) * limit
        
        log_event("INFO", "Consulta de datos de sensores", 
                 client_ip=request.remote_addr,
                 page=page,
                 limit=limit)
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        union_query = """
            SELECT * FROM (
                SELECT * FROM sensor_data
                UNION ALL
                SELECT * FROM sensor_data2
                UNION ALL
                SELECT * FROM sensor_data3
                UNION ALL
                SELECT * FROM sensor_data4
            ) AS todos_los_sensores
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(union_query, (limit, offset))
        sensores = cursor.fetchall()
        
        log_event("INFO", f"Datos de sensores obtenidos", 
                 records_count=len(sensores))
        
        cursor.close()
        conn.close()
        
        return jsonify(sensores), 200
        
    except Exception as e:
        log_event("ERROR", "Error al obtener datos de sensores", error=str(e))
        return jsonify({"status": "fail", "message": f"Error: {e}"}), 500

# Script para inicializar la tabla de logs si no existe
def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Crear tabla de logs si no existe
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS log_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            level VARCHAR(10) NOT NULL,
            message TEXT NOT NULL,
            extra_data JSON,
            timestamp DATETIME NOT NULL
        )
        """)
        
        conn.commit()
        print("[INIT] ✅ Tabla de logs configurada correctamente")
        
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as e:
        print(f"[INIT] ❌ Error al configurar la base de datos: {e}")

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Estación Meteorológica MQTT con Logs")
    parser.add_argument("--mode", choices=["publisher", "subscriber", "server"], required=True)
    parser.add_argument("--stations", nargs="*", help="IDs de las estaciones")
    parser.add_argument("--topic", default="/estacion/+/sensores")
    parser.add_argument("--port", type=int, default=5000, help="Puerto para el servidor Flask")
    parser.add_argument("--host", default="0.0.0.0", help="Host para el servidor Flask")
    args = parser.parse_args()

    # Inicializar la tabla de logs
    init_db()

    if args.mode == "publisher":
        estaciones = args.stations if args.stations else ["station1", "station2", "station3", "station4"]
        log_event("INFO", "Iniciando simulación de estaciones", stations=estaciones)
        start_multiple_stations(estaciones)
    elif args.mode == "subscriber":
        log_event("INFO", "Iniciando subscriber MQTT", topic=args.topic)
        Subscriber("subscriber-1").start_listening(args.topic)
    elif args.mode == "server":
        log_event("INFO", "Iniciando servidor Flask", host=args.host, port=args.port)
        app.run(host=args.host, port=args.port, debug=True)

        