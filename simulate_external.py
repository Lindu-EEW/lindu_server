import paho.mqtt.client as mqtt
import json, time

client = mqtt.Client()
client.connect("localhost", 1883, 60)
client.loop_start()

payload = {
    "source": "JMA (Jepang)",
    "lat": 35.6895,
    "lon": 139.6917,
    "radius_km": 150,
    "confidence": 99,
    "magnitude": 7.3,
    "desc": "⚠️ Peringatan Dini Eksternal! Sistem terintegrasi dengan server Japan Meteorological Agency (JMA). Telah terjadi gempa tektonik Skala Shindo 6 Bawah.",
    "metrics": [
        {"id": "JMA_ST_TOKYO", "val": "1.2G"},
        {"id": "JMA_ST_CHIBA", "val": "0.9G"},
        {"id": "JMA_TSUNAMI_BUOY", "val": "WAVE_DETECT"}
    ]
}

print("[EXT] Mengirim peringatan dari JMA...")
client.publish("lindu/external/alert", json.dumps(payload))
time.sleep(1)
print("[EXT] Terkirim! Periksa dasbor Anda.")
client.loop_stop()
client.disconnect()
