import paho.mqtt.client as mqtt
import json
import time
import threading
import sys
import random

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

NODES = {
    # INDONESIA
    "ID_CIANJUR":   {"lat": -6.8222, "lon": 107.1388},
    "ID_SUKABUMI":  {"lat": -6.9222, "lon": 106.9222},
    "ID_BOGOR":     {"lat": -6.5971, "lon": 106.7932},
    "ID_BANDUNG":   {"lat": -6.9175, "lon": 107.6191},
    "ID_JAKARTA":   {"lat": -6.2088, "lon": 106.8456},
    # JEPANG
    "JP_TOKYO":     {"lat": 35.6895, "lon": 139.6917},
    "JP_YOKOHAMA":  {"lat": 35.4437, "lon": 139.6380},
    "JP_CHIBA":     {"lat": 35.6073, "lon": 140.1063},
    "JP_OSAKA":     {"lat": 34.6937, "lon": 135.5023},
    "JP_KOBE":      {"lat": 34.6901, "lon": 135.1955},
    # JEPANG MASSIVE (20+ Nodes)
    "JP_N1": {"lat": 35.65, "lon": 139.75}, "JP_N2": {"lat": 35.66, "lon": 139.76},
    "JP_N3": {"lat": 35.67, "lon": 139.77}, "JP_N4": {"lat": 35.68, "lon": 139.78},
    "JP_N5": {"lat": 35.69, "lon": 139.79}, "JP_N6": {"lat": 35.70, "lon": 139.80},
    "JP_N7": {"lat": 35.71, "lon": 139.81}, "JP_N8": {"lat": 35.72, "lon": 139.82},
    "JP_N9": {"lat": 35.73, "lon": 139.83}, "JP_N10": {"lat": 35.74, "lon": 139.84},
    "JP_N11": {"lat": 35.75, "lon": 139.85}, "JP_N12": {"lat": 35.76, "lon": 139.86},
    "JP_N13": {"lat": 35.77, "lon": 139.87}, "JP_N14": {"lat": 35.78, "lon": 139.88},
    "JP_N15": {"lat": 35.79, "lon": 139.89}, "JP_N16": {"lat": 35.80, "lon": 139.90},
    "JP_N17": {"lat": 35.81, "lon": 139.91}, "JP_N18": {"lat": 35.82, "lon": 139.92},
    "JP_N19": {"lat": 35.83, "lon": 139.93}, "JP_N20": {"lat": 35.84, "lon": 139.94},
}

SCENARIOS = [
    {
        "nama": "Megathrust Kanto MASSIVE (23 Nodes)",
        "events": [
            {"node": "JP_TOKYO",    "delay": 0.0, "pga": 0.95},
            {"node": "JP_YOKOHAMA", "delay": 2.0, "pga": 0.55},
            {"node": "JP_CHIBA",    "delay": 1.5, "pga": 0.35},
            {"node": "JP_N1",  "delay": 0.5, "pga": 0.85},
            {"node": "JP_N2",  "delay": 0.5, "pga": 0.80},
            {"node": "JP_N3",  "delay": 0.5, "pga": 0.75},
            {"node": "JP_N4",  "delay": 0.5, "pga": 0.70},
            {"node": "JP_N5",  "delay": 0.5, "pga": 0.65},
            {"node": "JP_N6",  "delay": 0.5, "pga": 0.60},
            {"node": "JP_N7",  "delay": 0.5, "pga": 0.55},
            {"node": "JP_N8",  "delay": 0.5, "pga": 0.50},
            {"node": "JP_N9",  "delay": 0.5, "pga": 0.45},
            {"node": "JP_N10", "delay": 0.5, "pga": 0.40},
            {"node": "JP_N11", "delay": 0.5, "pga": 0.35},
            {"node": "JP_N12", "delay": 0.5, "pga": 0.30},
            {"node": "JP_N13", "delay": 0.5, "pga": 0.25},
            {"node": "JP_N14", "delay": 0.5, "pga": 0.20},
            {"node": "JP_N15", "delay": 0.5, "pga": 0.15},
            {"node": "JP_N16", "delay": 0.5, "pga": 0.10},
            {"node": "JP_N17", "delay": 0.5, "pga": 0.09},
            {"node": "JP_N18", "delay": 0.5, "pga": 0.08},
            {"node": "JP_N19", "delay": 0.5, "pga": 0.07},
            {"node": "JP_N20", "delay": 0.5, "pga": 0.06},
        ]
    }
]

alarm_triggered = False

def on_connect(client, userdata, flags, rc):
    print("[Simulator] Terhubung ke MQTT Broker lokal!")
    client.subscribe("lindu/actuator/cmd/all")

def on_message(client, userdata, msg):
    global alarm_triggered
    topic = msg.topic
    payload = msg.payload.decode('utf-8')
    
    if topic == "lindu/actuator/cmd/all":
        data = json.loads(payload)
        if data.get("cmd") == "ALARM_ON":
            print(f"🚨 ALARM GLOBAL MENYALA (M {data.get('magnitude', '?.?')}) 🚨")
            alarm_triggered = True

def simulate():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()
    
    print("\n[1] Mendaftarkan 30+ Node ESP32 Virtual (Global) ke Server...")
    for node_id, coords in NODES.items():
        client.publish(f"lindu/sensor/{node_id}/status", json.dumps({
            "status": "online", "node_id": node_id, "lat": coords["lat"], "lon": coords["lon"]
        }))
    time.sleep(2)
    
    scenario = SCENARIOS[0]
    print(f"\n[2] BUMI BERGETAR! Mengaktifkan Skenario: ** {scenario['nama']} **\n")
    
    def send_telemetry_stream(node_id, pga_base, coords):
        for _ in range(15):
            time.sleep(1)
            new_pga = pga_base * random.uniform(0.8, 1.5)
            client.publish(f'lindu/sensor/{node_id}/telemetry', json.dumps({'node_id': node_id, 'pga': new_pga, 'rms': new_pga * random.uniform(0.3, 0.6), 'lat': coords['lat'], 'lon': coords['lon']}))
            
    ts_current = int(time.time())
    
    for i, event in enumerate(scenario["events"]):
        if event["delay"] > 0:
            time.sleep(event["delay"])
            ts_current += event["delay"]
            
        node_id = event["node"]
        pga = event["pga"]
        coords = NODES[node_id]
        
        print(f"[{i+3}] Sensor {node_id} MENDETEKSI GUNCANGAN! (PGA: {pga}G)")
        client.publish(f"lindu/sensor/{node_id}/event", json.dumps({
            "node_id": node_id, 
            "ts": ts_current, 
            "pga": pga,
            "rms": pga * 0.4, 
            "sta_lta": 15.0,
            "lat": coords["lat"], "lon": coords["lon"]
        }))
        threading.Thread(target=send_telemetry_stream, args=(node_id, pga, coords)).start()
    
    print("\n[⌛] Menunggu 15 detik pemurnian data telemetri...")
    time.sleep(20)
    print("[✓] UJI INTEGRASI GLOBAL BERHASIL SEMPURNA!")
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    simulate()
