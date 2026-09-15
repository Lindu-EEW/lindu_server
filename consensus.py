import json
import math
import psutil
import sys
import os
import time
import math
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

# Konfigurasi MQTT
MQTT_BROKER = os.getenv("MQTT_BROKER", "192.168.68.105")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
TOPIC_STATUS = "lindu/sensor/+/status"
TOPIC_EVENT  = "lindu/sensor/+/event"
TOPIC_TELEMETRY = "lindu/sensor/+/telemetry"
TOPIC_EXTERNAL  = "lindu/external/alert"
TOPIC_ALARM  = "lindu/actuator/cmd/all"

# Konfigurasi Database
DB_HOST = "grafana_postgres"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_NAME = "lindu_db"

# In-Memory Database (Cache Cepat agar tidak lag saat gempa)
node_registry = {} 
trigger_buffer = []
active_quake = None
mqtt_client = None

def init_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Tabel Master Node
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tb_nodes (
                node_id VARCHAR(32) PRIMARY KEY,
                role VARCHAR(16),
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                registered_at TIMESTAMPTZ DEFAULT NOW(),
                last_seen TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        
        # Tabel Event Ringkasan (Saat Konsensus Tercapai)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tb_sensor_events (
                time_event TIMESTAMPTZ(3) NOT NULL DEFAULT NOW(),
                node_id VARCHAR(32),
                pga FLOAT,
                sta_lta FLOAT,
                freq_hz INTEGER,
                is_confirmed BOOLEAN DEFAULT FALSE
            );
        """)
        
        # Tabel Telemetri Streaming (Rekaman Detik-per-Detik Selama Gempa)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tb_sensor_telemetry (
                ts TIMESTAMPTZ(3) NOT NULL DEFAULT NOW(),
                node_id VARCHAR(32),
                pga FLOAT,
                sta_lta FLOAT,
                freq_hz INTEGER,
                ax FLOAT,
                ay FLOAT,
                az FLOAT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                uptime_ms BIGINT
            );
        """)
        
        # Tabel Event Ringkasan (Saat Konsensus Tercapai)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tb_system_alerts (
                time_alert TIMESTAMPTZ(3) NOT NULL DEFAULT NOW(),
                source VARCHAR(32),
                epi_lat DOUBLE PRECISION,
                epi_lon DOUBLE PRECISION,
                radius_km FLOAT,
                magnitude FLOAT,
                measured_velocity_kms FLOAT,
                triggering_nodes JSONB,
                seismic_details JSONB,
                description TEXT
            );
        """)
        
        # Migrasi kolom baru (jika tabel sudah terlanjur dibuat di versi sebelumnya)
        cur.execute("ALTER TABLE tb_system_alerts ADD COLUMN IF NOT EXISTS magnitude FLOAT;")
        cur.execute("ALTER TABLE tb_system_alerts ADD COLUMN IF NOT EXISTS measured_velocity_kms FLOAT;")
        cur.execute("ALTER TABLE tb_system_alerts ADD COLUMN IF NOT EXISTS triggering_nodes JSONB;")
        cur.execute("ALTER TABLE tb_system_alerts ADD COLUMN IF NOT EXISTS seismic_details JSONB;")
        
        cur.execute("ALTER TABLE tb_nodes ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ DEFAULT NOW();")
        print("[DB] Semua tabel dipastikan ada.")
        
        # Load memori dari DB agar tidak amnesia setelah restart
        cur.execute("SELECT node_id, lat, lon FROM tb_nodes;")
        for row in cur.fetchall():
            node_registry[row[0]] = {"lat": row[1], "lon": row[2], "last_seen": time.time()}
        print(f"[DB] {len(node_registry)} Node berhasil di-load dari Database ke RAM.")
        
        cur.close()
        release_db_connection(conn)
    except Exception as e:
        print(f"[!] Gagal konek DB (Abaikan jika testing lokal tanpa DB): {e}")

# Global Connection Pool
db_pool = None
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
except Exception as e:
    print("Gagal membuat Connection Pool:", e)

def get_db_connection():
    global db_pool
    if db_pool:
        try:
            conn = db_pool.getconn()
            # PING TEST: Pastikan koneksi tidak basi (stale)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            except Exception:
                print("[DB WARN] Koneksi basi terdeteksi! Membangun ulang Connection Pool...")
                db_pool.closeall()
                import psycopg2
                import psycopg2.pool
                from config import DB_HOST, DB_NAME, DB_USER, DB_PASS
                db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                conn = db_pool.getconn()
            return conn
        except Exception as e:
            print("[DB ERROR] Gagal mengambil koneksi dari pool:", e)
            return None
    return None

def release_db_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

def save_telemetry(payload):
    """Simpan setiap pesan telemetri streaming ke database untuk analisis post-event."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        
        # Konversi Unix Epoch ke TIMESTAMPTZ secara aman
        ts_value = payload.get("ts", 0)
        if ts_value and ts_value > 1000000:
            cur.execute("""
                INSERT INTO tb_sensor_telemetry (ts, node_id, pga, sta_lta, freq_hz, ax, ay, az, lat, lon, uptime_ms)
                VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                ts_value,
                payload.get("node_id"),
                payload.get("pga", 0),
                payload.get("sta_lta", 0),
                payload.get("freq_hz", 0),
                payload.get("ax", 0),
                payload.get("ay", 0),
                payload.get("az", 0),
                payload.get("lat", 0),
                payload.get("lon", 0),
                payload.get("uptime", 0)
            ))
        else:
            cur.execute("""
                INSERT INTO tb_sensor_telemetry (ts, node_id, pga, sta_lta, freq_hz, ax, ay, az, lat, lon, uptime_ms)
                VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                payload.get("node_id"),
                payload.get("pga", 0),
                payload.get("sta_lta", 0),
                payload.get("freq_hz", 0),
                payload.get("ax", 0),
                payload.get("ay", 0),
                payload.get("az", 0),
                payload.get("lat", 0),
                payload.get("lon", 0),
                payload.get("uptime", 0)
            ))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB Telemetry Error] {e}")
    finally:
        release_db_connection(conn)

def save_alert(alarm_payload):
    """Simpan riwayat alarm ke database (Black Box)."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        import json
        cur = conn.cursor()
        nodes_json = json.dumps(alarm_payload.get("triggering_nodes", []))
        details_json = json.dumps({
            "time_diff_s": alarm_payload.get("time_diff_s", 0),
            "confidence": alarm_payload.get("confidence", 0)
        })
        
        cur.execute("""
            INSERT INTO tb_system_alerts (
                source, epi_lat, epi_lon, radius_km, 
                magnitude, measured_velocity_kms, triggering_nodes, seismic_details, description
            )
            VALUES ('LOCAL_CONSENSUS', %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            alarm_payload["epicenter_lat"],
            alarm_payload["epicenter_lon"],
            alarm_payload["radius_km"],
            alarm_payload.get("magnitude", 0),
            alarm_payload.get("measured_velocity_kms", 0),
            nodes_json,
            details_json,
            alarm_payload["desc"]
        ))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB Alert Error] {e}")
    finally:
        release_db_connection(conn)

def haversine_km(lat1, lon1, lat2, lon2):
    """Menghitung jarak bumi dalam kilometer"""
    R = 6371.0 
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def on_connect(client, userdata, flags, rc):
    print(f"[SERVER] Terhubung ke MQTT Broker dengan kode {rc}")
    client.subscribe([(TOPIC_STATUS, 0), (TOPIC_EVENT, 0), (TOPIC_TELEMETRY, 0), (TOPIC_EXTERNAL, 0)])
    print(f"[SERVER] Listening to {TOPIC_STATUS}, {TOPIC_EVENT}, {TOPIC_TELEMETRY}...")

def on_message(client, userdata, msg):
    global trigger_buffer
    
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
    except json.JSONDecodeError:
        return

    # 1. UPSERT NODE REGISTRY (Dari Heartbeat / Status)
    if "/log" in msg.topic:
        try:
            node_id = msg.topic.split('/')[2]
            log_msg = payload.get("message", "Unknown log")
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO tb_node_logs (node_id, message) VALUES (%s, %s)", (node_id, log_msg))
                    conn.commit()
                    cur.execute("DELETE FROM tb_node_logs WHERE ts < NOW() - INTERVAL '3 days'")
                    conn.commit()
                finally:
                    release_db_connection(conn)
        except Exception as e:
            print("[LOG ERROR]", e)
        return

    if "/status" in msg.topic:
        # Fallback node_id dari topic jika payload tidak ada
        node_id = payload.get("node_id") or msg.topic.split('/')[2]
        lat = payload.get("lat")
        lon = payload.get("lon")
        
        if node_id and lat is not None and lon is not None:
            node_registry[node_id] = {
                "lat": lat,
                "lon": lon,
                "last_seen": time.time()
            }
            
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO tb_nodes (node_id, role, lat, lon, last_seen) 
                        VALUES (%s, 'sensor', %s, %s, NOW())
                        ON CONFLICT (node_id) 
                        DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon, last_seen = NOW();
                    """, (node_id, lat, lon))
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[DB Error] {e}")
                finally:
                    release_db_connection(conn)

            
    # 2. EVENT / TELEMETRI (Ada Getaran dari Node)
    elif "/telemetry" in msg.topic:
        node_id = payload.get("node_id")
        pga = payload.get("pga", 0)
        sta_lta = payload.get("sta_lta", 0)
        
        freq_hz = payload.get("freq_hz", 0)
        
        # [ALGORITMA ANTI-HOAKS / FILTER GETARAN KAKI]
        # 1. PGA >= 0.12 (Getaran harus cukup keras)
        # 2. STA/LTA >= 2.0 (Energi getaran harus berkelanjutan, bukan benturan singkat)
        # 3. Frekuensi <= 20 Hz (Gelombang seismik bumi, bukan ketukan/hentakan sepatu yang tinggi)
        is_real_quake = (pga >= 0.12 and sta_lta >= 2.0 and freq_hz <= 20)
        
        if not is_real_quake:
            return # Buang hentakan kaki, buku jatuh, dan noise kecil
        
        # Simpan SETIAP pesan telemetri ke database (rekaman detik-per-detik)
        save_telemetry(payload)
        
        # Gunakan koordinat dari payload jika ada, fallback ke registry
        lat = payload.get("lat")
        lon = payload.get("lon")
        if lat is None or lon is None:
            if node_id in node_registry:
                lat = node_registry[node_id]["lat"]
                lon = node_registry[node_id]["lon"]
            else:
                print(f"[WARNING] Event dari {node_id} diabaikan (Lokasi tidak diketahui)")
                return
            
        print(f"[TELE] {node_id} | PGA:{pga:.3f}G | STA/LTA:{sta_lta:.1f} | {payload.get('freq_hz',0)}Hz")
        
        new_trigger = {
            "node_id": node_id,
            "timestamp": payload.get("ts") or time.time(), # FIX: Gunakan epoch sensor
            "lat": lat,
            "lon": lon,
            "pga": pga
        }
        trigger_buffer.append(new_trigger)
        run_consensus_logic()
    # [SISIPAN] 4. HANDLE TELEMETRY UNTUK REFINEMENT EPISENTRUM (LIVE UPDATE)
    if "/telemetry" in msg.topic:
        node_id = payload.get("node_id")
        pga = payload.get("pga", 0)
        lat = payload.get("lat")
        lon = payload.get("lon")
        
        if node_id and lat and lon:
            # Simpan raw telemetry ke database
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO tb_telemetry (node_id, pga, sta_lta, freq_hz)
                        VALUES (%s, %s, %s, %s)
                    """, (node_id, pga, payload.get("sta_lta", 0), payload.get("freq_hz", 0)))
                    conn.commit()
                    cur.close()
                except Exception:
                    pass
                finally:
                    release_db_connection(conn)
            
            # Refine active earthquake if within 15 seconds window
            global active_quake
            if active_quake and (time.time() - active_quake["start_time"]) <= 15.0:
                # Update max PGA for this node
                
                active_quake["telemetry_count"] += 1
                active_quake["nodes_pga"][node_id] = max(active_quake["nodes_pga"].get(node_id, 0.0), pga)
                active_quake["nodes_coords"][node_id] = (lat, lon)
                
                # Calculate Weighted Center of Energy (Epicenter)
                sum_pga = 0
                sum_lat = 0
                sum_lon = 0
                for nid, max_p in active_quake["nodes_pga"].items():
                    c_lat, c_lon = active_quake["nodes_coords"][nid]
                    sum_lat += c_lat * max_p
                    sum_lon += c_lon * max_p
                    sum_pga += max_p
                
                if sum_pga > 0:
                    refined_lat = sum_lat / sum_pga
                    refined_lon = sum_lon / sum_pga
                    
                    # Refine Magnitude using absolute Max PGA across all reporting nodes
                    abs_max_pga = max(active_quake["nodes_pga"].values())
                    refined_mag = 5.0 + 1.5 * math.log10(abs_max_pga / 0.1) if abs_max_pga >= 0.1 else 5.0
                    refined_mag = max(5.0, min(refined_mag, 9.9))
                    refined_radius = 10 ** (0.5 * refined_mag - 1.0)
                    
                    # Publish Update
                    
                    t_nodes = []
                    for nid, max_p in active_quake["nodes_pga"].items():
                        c_lat, c_lon = active_quake["nodes_coords"][nid]
                        t_nodes.append({
                            "id": nid, "lat": c_lat, "lon": c_lon, "pga": round(max_p, 3)
                        })
                    
                    update_payload = {
                        "triggering_nodes": t_nodes,

                        "cmd": "ALARM_UPDATE",
                        "epicenter_lat": refined_lat,
                        "epicenter_lon": refined_lon,
                        "magnitude": round(refined_mag, 1),
                        "radius_km": round(refined_radius, 1),
                        "shift_km": round(haversine_km(active_quake["initial_lat"], active_quake["initial_lon"], refined_lat, refined_lon), 2),
                        "telemetry_count": active_quake["telemetry_count"],
                        "desc": f"📡 Pemurnian Episentrum Live! Mengolah data dari {len(active_quake['nodes_pga'])} sensor aktif. Pusat energi terkoreksi menuju titik guncangan maksimum (PGA {abs_max_pga:.2f}G)."
                    }
                    mqtt_client.publish(TOPIC_ALARM, json.dumps(update_payload))
                    print(f"[LIVE REFINEMENT] Pusat: {refined_lat:.4f}, {refined_lon:.4f} | Mag: {refined_mag:.1f} | Nodes: {len(active_quake['nodes_pga'])}")
                    
                    # Update database agar Grafana menampilkan magnitudo dan radius yang sudah direvisi
                    try:
                        conn_update = get_db_connection()
                        if conn_update:
                            cur_update = conn_update.cursor()
                            cur_update.execute("""
                                UPDATE tb_system_alerts 
                                SET magnitude = %s, radius_km = %s, epi_lat = %s, epi_lon = %s, description = %s, triggering_nodes = %s
                                WHERE time_alert = (SELECT time_alert FROM tb_system_alerts ORDER BY time_alert DESC LIMIT 1)
                                RETURNING time_alert;
                            """, (
                                round(refined_mag, 1), 
                                round(refined_radius, 1), 
                                refined_lat, 
                                refined_lon, 
                                update_payload["desc"],
                                json.dumps(t_nodes)
                            ))
                            conn_update.commit()
                    except Exception as e:
                        print("Gagal update DB Refinement:", e)
                    finally:
                        release_db_connection(conn_update)



# ==========================================

# BACKEND API (UNTUK DASHBOARD)
# ==========================================

from flask import Flask, jsonify
from flask_cors import CORS
import threading

app = Flask(__name__)
CORS(app)

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "status": "online",
        "service": "Lindu.id Consensus & API Engine",
        "endpoints": [
            "/api/history",
            "/api/nodes",
            "/api/metrics",
            "/api/cmd"
        ]
    })

@app.route('/api/history', methods=['GET'])
def get_history():
    """Mengambil riwayat gempa dari database untuk ditampilkan di Peta Dashboard"""
    conn = get_db_connection()
    if not conn:
        return jsonify([]), 200
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT time_alert, epi_lat, epi_lon, radius_km, description, magnitude, measured_velocity_kms, triggering_nodes 
            FROM tb_system_alerts 
            ORDER BY time_alert ASC LIMIT 50;
        """)
        rows = cur.fetchall()
        
        history = []
        for r in rows:
            time_str = r[0].strftime("%H:%M:%S")
            history.append({
                "time": time_str,
                "lat": r[1],
                "lon": r[2],
                "radius": r[3],
                "desc": r[4],
                "magnitude": r[5] or 0.0,
                "velocity": r[6] or 0.0,
                "triggering_nodes": r[7] or []
            })
        cur.close()
        return jsonify(history)
    except Exception as e:
        print(f"[API Error] {e}")
        return jsonify([]), 500
    finally:
        release_db_connection(conn)

def run_consensus_logic():

    global trigger_buffer
    now = time.time()
    
    # Hapus trigger yang usianya lebih dari 60 detik (Expired)
    trigger_buffer = [t for t in trigger_buffer if (now - t["timestamp"]) <= 60.0]
    
    # Himpun node unik
    unique_nodes = {}
    for t in trigger_buffer:
        if t["node_id"] not in unique_nodes:
            unique_nodes[t["node_id"]] = t
            
    nodes = list(unique_nodes.values())
    
    if len(nodes) < 2:
        return
        
    import itertools
    # Evaluasi setiap kemungkinan pasang node dalam buffer
    for t1, t2 in itertools.combinations(nodes, 2):
        jarak_km = haversine_km(t1["lat"], t1["lon"], t2["lat"], t2["lon"])
        selisih_waktu = abs(t2["timestamp"] - t1["timestamp"])
        
        # Jika jarak terlalu dekat (misal di ruangan yang sama), anggap valid langsung
        if jarak_km < 0.1:
            print(f"[KONSENSUS] 2 Node di area yang sama bergetar. Validasi Fisika di-bypass.")
            fire_alarm(mqtt_client, t1, t2, 0.0, 0.0)
            return
            
        # HUKUM FISIKA & KETERBATASAN WAKTU
        print(f"[*] Mengevaluasi {t1['node_id']} & {t2['node_id']}")
        
        is_valid = False
        if selisih_waktu == 0:
            # 1. Episentrum berada persis di tengah kedua node (equidistant)
            # 2. Rambatan terjadi di bawah 1 detik (karena presisi timestamp ESP32 hanya 1 detik)
            kecepatan = 6.0 # Asumsi kecepatan standar P-Wave
            is_valid = True
            print(f"    Jarak: {jarak_km:.2f} km | Δt: 0.000s -> EPISENTRUM EQUIDISTANT / SIMULTAN")
            print(f"[+] KONSENSUS TERCAPAI! Gelombang tiba bersamaan di kedua sensor.")
        else:
            kecepatan = jarak_km / selisih_waktu
            print(f"    Jarak: {jarak_km:.2f} km | Δt: {selisih_waktu:.3f}s | Kecepatan: {kecepatan:.2f} km/s")
            # Toleransi hingga 25 km/s untuk kompensasi pembulatan detik
            if kecepatan > 0.5 and kecepatan < 25.0:
                is_valid = True
                print(f"[+] KONSENSUS TERCAPAI! Jarak {jarak_km:.2f} KM (Kecepatan {kecepatan:.2f} km/s).")
            else:
                print(f"    [-] Validasi pair gagal. Kecepatan {kecepatan:.2f} km/s tidak masuk akal.")
                
        if is_valid:
            fire_alarm(mqtt_client, t1, t2, kecepatan, selisih_waktu)
            return  # Selesai, buffer sudah dikosongkan di fire_alarm
            
    # Jika sampai di sini, artinya tidak ada pasangan yang valid (semua False Positive). 
    # Mereka akan terus tertahan di buffer sampai expired (60 detik).
    print(f"[-] VALIDASI GAGAL! Tidak ada pasangan node yang masuk akal secara fisika dari {len(nodes)} node.")

last_alarm_time = 0  # Cooldown tracker

def fire_alarm(client, t1, t2, velocity, time_diff):
    global active_quake, last_alarm_time
    import time
    
    # COOLDOWN 60 DETIK: Jika alarm sudah pernah dipicu < 60 detik lalu,
    # JANGAN membuat alarm baru. Biarkan Live Refinement yang bekerja.
    if time.time() - last_alarm_time < 30:
        print("[COOLDOWN] Alarm diabaikan. Gempa ini masih dalam jendela pemurnian 30 detik.")
        return
    
    last_alarm_time = time.time()
    active_quake = {
        'start_time': time.time(),
        'initial_lat': (t1["lat"] + t2["lat"]) / 2,
        'initial_lon': (t1["lon"] + t2["lon"]) / 2,
        'telemetry_count': 0,
        'nodes_pga': {t1['node_id']: t1.get('pga',0.1), t2['node_id']: t2.get('pga',0.1)},
        'nodes_coords': {t1['node_id']: (t1['lat'], t1['lon']), t2['node_id']: (t2['lat'], t2['lon'])}
    }
    global trigger_buffer
    import math
    
    # Episentrum diprediksi berada di tengah-tengah kedua node (Midpoint Approximation)
    epi_lat = (t1["lat"] + t2["lat"]) / 2
    epi_lon = (t1["lon"] + t2["lon"]) / 2
    
    # Estimasi Magnitudo berdasarkan rata-rata PGA
    avg_pga = (t1["pga"] + t2["pga"]) / 2.0
    safe_pga = max(0.001, avg_pga)
    magnitude = round(5.0 + 1.5 * math.log10(safe_pga / 0.1), 1)
    magnitude = max(3.0, min(9.5, magnitude))
    
    # Radius Bahaya Dinamis Berdasarkan Magnitudo
    dynamic_radius = round(10 ** (0.5 * magnitude - 1.0), 1)
    
    alarm_payload = {
        "cmd": "trigger_siren",
        "level": "CRITICAL",
        "epicenter_lat": epi_lat,
        "epicenter_lon": epi_lon,
        "radius_km": dynamic_radius,
        "confidence": 95,
        "magnitude": magnitude,
        "measured_velocity_kms": round(velocity, 2),
        "time_diff_s": round(time_diff, 2),
        "triggering_nodes": [
            {"id": t1["node_id"], "lat": t1["lat"], "lon": t1["lon"], "pga": t1.get("pga", 0)},
            {"id": t2["node_id"], "lat": t2["lat"], "lon": t2["lon"], "pga": t2.get("pga", 0)}
        ],
        "desc": f"⚠️ Peringatan Dini! Sensor {t1['node_id']} mencatat guncangan (PGA {t1.get('pga',0):.2f}G), divalidasi oleh {t2['node_id']} ({t2.get('pga',0):.2f}G) dalam {time_diff:.1f} detik. Rambatan P-Wave terukur {velocity:.1f} km/s (Fisika Valid)."
    }
    
    print("\n=======================================================")
    print(f"🚨 KONSENSUS TERCAPAI! GEMPA M {magnitude} TERDETEKSI! 🚨")
    print(json.dumps(alarm_payload, indent=2))
    print("=======================================================\n")
    
    mqtt_client.publish(TOPIC_ALARM, json.dumps(alarm_payload))
    
    # Simpan ke database (Black Box)
    save_alert(alarm_payload)
    
    # Kosongkan buffer agar tidak spam alarm berkali-kali
    trigger_buffer.clear()

from flask import request

@app.route('/api/cmd', methods=['POST'])
def send_cmd():
    try:
        data = request.json
        cmd = data.get('cmd')
        target = data.get('target_node', 'all')
        
        if not cmd:
            return jsonify({"error": "Missing cmd parameter"}), 400
            
        payload = {"cmd": cmd, "target_node": target}
        
        if mqtt_client:
            mqtt_mqtt_client.publish(TOPIC_ALARM, json.dumps(payload))
            return jsonify({"status": "success", "message": f"Command '{cmd}' sent to {target}"})
        else:
            return jsonify({"error": "MQTT Client not initialized"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    conn = get_db_connection()
    if not conn:
        return jsonify([])
    try:
        cur = conn.cursor()
        # Mengambil data status terakhir untuk setiap node
        cur.execute("""
            SELECT DISTINCT ON (node_id) 
                node_id, time, status, fw_version, ota_status, latency_ms, sensor_ok 
            FROM sensor_status 
            ORDER BY node_id, time DESC;
        """)
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append({
                "node_id": r[0],
                "time": r[1].isoformat() if r[1] else None,
                "status": r[2],
                "fw_version": r[3] if r[3] else "UNKNOWN",
                "ota_status": r[4] if r[4] else "IDLE",
                "latency_ms": r[5],
                "sensor_ok": r[6]
            })
        return jsonify(result)
    except Exception as e:
        print(f"DB Error (Metrics): {e}")
        return jsonify([])
    finally:
        release_db_connection(conn)

@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    """Mengambil semua node yang pernah teregistrasi di database"""
    conn = get_db_connection()
    if not conn:
        return jsonify([]), 200
    try:
        cur = conn.cursor()
        cur.execute("SELECT node_id, lat, lon, last_seen FROM tb_nodes;")
        rows = cur.fetchall()
        
        nodes = []
        for r in rows:
            nodes.append({
                "node_id": r[0],
                "lat": r[1],
                "lon": r[2],
                "last_seen": r[3].strftime("%Y-%m-%d %H:%M:%S") if r[3] else None
            })
        cur.close()
        return jsonify(nodes)
    except Exception as e:
        print(f"[API Nodes Error] {e}")
        return jsonify([]), 500
    finally:
        release_db_connection(conn)

if __name__ == "__main__":
    print("=======================================================")
    print("🌋 LINDU.ID - ENTERPRISE CONSENSUS ENGINE & API RUNNING")
    print("=======================================================")
    
    # Inisialisasi Database (Load memori)
    init_db()
    
    # Setup MQTT
    mqtt_client = mqtt.Client("LinduServer_01")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    print("Menyambungkan ke MQTT Broker...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        # Gunakan loop_start() (Background Thread) agar tidak memblokir Flask
        mqtt_client.loop_start() 
    except Exception as e:
        print(f"[!] Gagal terhubung ke MQTT Broker: {e}")
        
    # Jalankan REST API di Foreground
    print("[SERVER] Menjalankan API Backend di Port 5000...")
    app.run(host="0.0.0.0", port=5000, debug=False)


