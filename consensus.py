import json
import math
import os
import time
import threading
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.pool
from psycopg2.extras import execute_values

# Konfigurasi MQTT
MQTT_BROKER = os.getenv("MQTT_BROKER", "192.168.68.105")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
TOPIC_STATUS = "lindu/sensor/+/status"
TOPIC_EVENT  = "lindu/sensor/+/event"
TOPIC_TELEMETRY = "lindu/sensor/+/telemetry"
TOPIC_EXTERNAL  = "lindu/external/alert"
TOPIC_ALARM     = "lindu/actuator/cmd/all"
TOPIC_LOG       = "lindu/sensor/+/log"
TOPIC_OFFLINE   = "lindu/sensor/+/offline_events"
TOPIC_ACTUATOR  = "lindu/sensor/+/actuator"

server_start_time = time.time()

# Konfigurasi Database
DB_HOST = os.getenv("DB_HOST", "grafana_postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_NAME = os.getenv("DB_NAME", "lindu_db")

node_registry = {}
trigger_buffer = []
active_quake = None
mqtt_client = None
last_alarm_time = 0.0
EVENT_COOLDOWN_SEC = 60.0    # Jendela deduplikasi: 1 Gempa hanya menghasilkan TEPAT 1 Master Alert
REFINEMENT_WINDOW_SEC = 60.0 # Jendela Live Refinement: telemetri selama 60 detik memurnikan gempa yang sama

# ==============================================================================
# TinyML Dual-Engine / Shadow Mode Server Fallback
# Menggunakan bobot model TinyML yang sama persis dengan yang ada pada firmware C++ ESP32
# (dataset/lindu_ml_model.h) untuk memproses telemetri dari node firmware lama (v1.4.95)
# ==============================================================================
ML_MEANS = [0.189372, 7.681914, 14.518250, 0.089389, 21.913950, 0.605856]
ML_STDS  = [0.228498, 7.404773, 12.862055, 0.117197, 19.507890, 0.228757]
ML_WEIGHTS = [-0.377860, 1.252306, -3.959957, 0.364124, 1.158558, -1.332820]
ML_BIAS = 0.143630

def predict_tinyml(pga, sta_lta, freq_hz, rms, duration_s=1.0, p_to_s=0.5):
    """
    Evaluates seismic event probability using embedded TinyML model.
    Menghitung probabilitas gempa secara real-time (0.00 - 1.00).
    """
    if pga < 0.0080:
        return round(0.01 + (pga / 0.0080) * 0.02, 3)
    effective_freq = freq_hz if freq_hz > 0 else (2.5 if pga >= 0.12 else 18.0)
    feats = [pga, sta_lta, effective_freq, rms, duration_s, p_to_s]
    z = ML_BIAS
    for i in range(6):
        norm = (feats[i] - ML_MEANS[i]) / ML_STDS[i]
        z += ML_WEIGHTS[i] * norm
    if z < -20.0:
        return 0.0
    if z > 20.0:
        return 1.0
    return round(1.0 / (1.0 + math.exp(-z)), 3)

def init_db():
    conn = None
    for attempt in range(15):
        try:
            conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
            break
        except Exception as e:
            if attempt == 14:
                print(f"[!] Gagal konek DB (Abaikan jika testing lokal tanpa DB): {e}")
                return
            time.sleep(1)
    try:
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
        cur.execute("ALTER TABLE tb_sensor_telemetry ADD COLUMN IF NOT EXISTS gas_raw INTEGER;")
        cur.execute("ALTER TABLE tb_sensor_telemetry ADD COLUMN IF NOT EXISTS gas_alert BOOLEAN;")
        cur.execute("ALTER TABLE tb_sensor_telemetry ADD COLUMN IF NOT EXISTS door_status VARCHAR(20);")
        cur.execute("ALTER TABLE tb_sensor_telemetry ADD COLUMN IF NOT EXISTS valve_status VARCHAR(20);")
        cur.execute("ALTER TABLE tb_sensor_telemetry ADD COLUMN IF NOT EXISTS ml_prob DOUBLE PRECISION DEFAULT 0.0;")
        cur.execute("ALTER TABLE tb_sensor_telemetry ADD COLUMN IF NOT EXISTS ml_agree BOOLEAN DEFAULT TRUE;")
        cur.execute("ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS ml_prob DOUBLE PRECISION DEFAULT 0.0;")
        cur.execute("ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS ml_agree BOOLEAN DEFAULT TRUE;")
        cur.execute("ALTER TABLE tb_sensor_telemetry ADD COLUMN IF NOT EXISTS vibe_class VARCHAR(32) DEFAULT 'AMBIENT_TENANG';")
        cur.execute("ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS vibe_class VARCHAR(32) DEFAULT 'AMBIENT_TENANG';")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_telemetry (
                time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                node_id VARCHAR(50) NOT NULL,
                pga DOUBLE PRECISION,
                rms DOUBLE PRECISION,
                accel_x DOUBLE PRECISION,
                accel_y DOUBLE PRECISION,
                accel_z DOUBLE PRECISION,
                temperature DOUBLE PRECISION,
                pressure DOUBLE PRECISION,
                humidity DOUBLE PRECISION,
                latency_ms DOUBLE PRECISION,
                valve_status VARCHAR(20),
                gas_raw INTEGER,
                gas_alert BOOLEAN
            );
            CREATE TABLE IF NOT EXISTS sensor_status (
                time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                node_id VARCHAR(50) NOT NULL,
                status VARCHAR(20),
                pose VARCHAR(20),
                tilt_angle DOUBLE PRECISION,
                latency_ms DOUBLE PRECISION,
                sensor_ok BOOLEAN,
                fw_version VARCHAR(50),
                ota_status VARCHAR(50),
                motion_detected BOOLEAN
            );
            ALTER TABLE sensor_status ADD COLUMN IF NOT EXISTS motion_detected BOOLEAN;
            CREATE TABLE IF NOT EXISTS tb_server_health (
                ts TIMESTAMPTZ DEFAULT NOW(),
                cpu_percent FLOAT,
                ram_percent FLOAT,
                uptime_hours FLOAT,
                status VARCHAR(32)
            );
            CREATE TABLE IF NOT EXISTS tb_node_logs (
                ts TIMESTAMPTZ DEFAULT NOW(),
                node_id VARCHAR(50),
                message TEXT
            );

            CREATE TABLE IF NOT EXISTS actuator_log (
                time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                node_id VARCHAR(50) NOT NULL,
                actuator VARCHAR(32) NOT NULL,
                state VARCHAR(32) NOT NULL,
                reason VARCHAR(64)
            );

            -- Index performa tinggi untuk Grafana dan TimescaleDB
            CREATE INDEX IF NOT EXISTS idx_sensor_telemetry_time_node ON sensor_telemetry (time DESC, node_id);
            CREATE INDEX IF NOT EXISTS idx_tb_sensor_telemetry_ts_node ON tb_sensor_telemetry (ts DESC, node_id);
            CREATE INDEX IF NOT EXISTS idx_sensor_status_time_node ON sensor_status (time DESC, node_id);
            CREATE INDEX IF NOT EXISTS idx_tb_node_logs_ts ON tb_node_logs (ts DESC);
            CREATE INDEX IF NOT EXISTS idx_tb_server_health_ts ON tb_server_health (ts DESC);
            CREATE INDEX IF NOT EXISTS idx_actuator_log_time_node ON actuator_log (time DESC, node_id);
        """)
        print("[DB] Semua tabel dan index performa dipastikan ada.")
        
        # Load memori dari DB agar tidak amnesia setelah restart
        cur.execute("SELECT node_id, lat, lon, COALESCE(group_id, 'cluster_chiba') FROM tb_nodes;")
        for row in cur.fetchall():
            node_registry[row[0]] = {"lat": row[1], "lon": row[2], "group_id": row[3], "last_seen": time.time()}
        print(f"[DB] {len(node_registry)} Node berhasil di-load dari Database ke RAM (termasuk Group ID).")
        
        cur.close()
        conn.close()
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
    if db_pool is None:
        try:
            db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        except Exception:
            return None
    if db_pool:
        try:
            conn = db_pool.getconn()
            # PING TEST: Pastikan koneksi tidak basi (stale)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            except Exception:
                print("[DB WARN] Koneksi basi terdeteksi! Membangun ulang Connection Pool...")
                try:
                    db_pool.closeall()
                except Exception:
                    pass
                db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
                conn = db_pool.getconn()
            return conn
        except Exception as e:
            print("[DB ERROR] Gagal mengambil koneksi dari pool:", e)
            return None
    return None

def release_db_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

# Buffer untuk continuous telemetry dan status ingestion (dipakai Grafana & Live Fleet)
tb_telemetry_ingest_buffer = []
telemetry_ingest_buffer = []
status_ingest_buffer = []
logs_ingest_buffer = []
actuator_ingest_buffer = []
ingest_lock = threading.Lock()

_last_cpu_idle = 0.0
_last_cpu_total = 0.0

def db_ingest_writer_thread():
    global _last_cpu_idle, _last_cpu_total
    last_health_ts = 0
    while True:
        time.sleep(0.5)
        with ingest_lock:
            local_tb_telemetry = tb_telemetry_ingest_buffer[:]
            local_telemetry = telemetry_ingest_buffer[:]
            local_status = status_ingest_buffer[:]
            local_logs = logs_ingest_buffer[:]
            local_actuator = actuator_ingest_buffer[:]
            tb_telemetry_ingest_buffer.clear()
            telemetry_ingest_buffer.clear()
            status_ingest_buffer.clear()
            logs_ingest_buffer.clear()
            actuator_ingest_buffer.clear()

        if local_tb_telemetry:
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    execute_values(cur, """
                        INSERT INTO tb_sensor_telemetry (ts, node_id, pga, sta_lta, freq_hz, ax, ay, az, lat, lon, uptime_ms, gas_raw, gas_alert, door_status, valve_status, ml_prob, ml_agree, vibe_class)
                        VALUES %s
                    """, local_tb_telemetry)
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[Ingest TB Telemetry Error] {e}")
                    conn.rollback()
                finally:
                    release_db_connection(conn)

        if local_telemetry:
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    execute_values(cur, """
                        INSERT INTO sensor_telemetry (time, node_id, pga, rms, accel_x, accel_y, accel_z, temperature, pressure, humidity, latency_ms, valve_status, gas_raw, gas_alert, ml_prob, ml_agree, vibe_class)
                        VALUES %s
                    """, local_telemetry)
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[Ingest Telemetry Error] {e}")
                    conn.rollback()
                finally:
                    release_db_connection(conn)

        if local_status:
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    execute_values(cur, """
                        INSERT INTO sensor_status (time, node_id, status, pose, tilt_angle, latency_ms, sensor_ok, fw_version, ota_status, motion_detected)
                        VALUES %s
                    """, local_status)
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[Ingest Status Error] {e}")
                    conn.rollback()
                finally:
                    release_db_connection(conn)

        if local_logs:
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    execute_values(cur, """
                        INSERT INTO tb_node_logs (ts, node_id, message)
                        VALUES %s
                    """, local_logs)
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[Ingest Logs Error] {e}")
                    conn.rollback()
                finally:
                    release_db_connection(conn)

        if local_actuator:
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    execute_values(cur, """
                        INSERT INTO actuator_log (time, node_id, actuator, state, reason)
                        VALUES %s
                    """, local_actuator)
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[Ingest Actuator Error] {e}")
                    conn.rollback()
                finally:
                    release_db_connection(conn)

        # Host Health Periodik setiap 15 detik untuk Grafana Host Health Panel
        now_ts = time.time()
        if now_ts - last_health_ts >= 15:
            last_health_ts = now_ts
            try:
                up_sec = now_ts - server_start_time
                try:
                    with open("/proc/uptime", "r") as f:
                        up_sec = float(f.readline().split()[0])
                except Exception:
                    pass
                uptime_hours = round(up_sec / 3600.0, 2)

                ram_pct = 40.0
                try:
                    mem = {}
                    with open("/proc/meminfo", "r") as f:
                        for l in f:
                            p = l.split(":")
                            if len(p) == 2:
                                mem[p[0].strip()] = float(p[1].strip().split()[0])
                    t = mem.get("MemTotal", 1.0)
                    a = mem.get("MemAvailable", mem.get("MemFree", 0.0))
                    ram_pct = round(((t - a) / t) * 100.0, 1)
                except Exception:
                    pass

                cpu_pct = 4.0
                try:
                    with open("/proc/stat", "r") as f:
                        fields = [float(x) for x in f.readline().split()[1:]]
                        idle = fields[3] + fields[4]
                        total = sum(fields)
                        if _last_cpu_total > 0:
                            idle_delta = idle - _last_cpu_idle
                            total_delta = total - _last_cpu_total
                            if total_delta > 0:
                                cpu_pct = round(max(0.5, min(100.0, 100.0 * (1.0 - idle_delta / total_delta))), 1)
                        _last_cpu_idle = idle
                        _last_cpu_total = total
                except Exception:
                    pass

                conn = get_db_connection()
                if conn:
                    try:
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO tb_server_health (ts, cpu_percent, ram_percent, uptime_hours)
                            VALUES (NOW(), %s, %s, %s);
                        """, (cpu_pct, ram_pct, uptime_hours))
                        conn.commit()
                        cur.close()
                    except Exception as e:
                        conn.rollback()
                    finally:
                        release_db_connection(conn)
            except Exception as e:
                print(f"[Host Health Error] {e}")

threading.Thread(target=db_ingest_writer_thread, daemon=True).start()

def save_telemetry(payload):
    """Simpan setiap pesan telemetri streaming ke buffer batch database untuk performa tinggi."""
    try:
        ml_prob = float(payload.get("ml_prob", 0.0) or 0.0)
        ml_agree = bool(payload.get("ml_agree", True))
        ts_value = payload.get("ts")
        # Gunakan server arrival time dengan presisi milidetik asli untuk live streaming
        # agar 10 sampel per detik memiliki jeda ~100ms terpisah, bukan bertumpuk di .000 yang sama
        if payload.get("is_offline_sync") and ts_value and float(ts_value) > 1000000:
            row_time = datetime.fromtimestamp(float(ts_value), tz=timezone.utc)
        else:
            row_time = datetime.now(timezone.utc)

        vibe_class = str(payload.get("vibe_class") or "AMBIENT_TENANG")
        row = (
            row_time,
            payload.get("node_id"),
            float(payload.get("pga", 0.0) or 0.0),
            float(payload.get("sta_lta", 0.0) or 0.0),
            int(payload.get("freq_hz", 0) or 0),
            float(payload.get("ax", 0.0) or 0.0),
            float(payload.get("ay", 0.0) or 0.0),
            float(payload.get("az", 0.0) or 0.0),
            float(payload.get("lat", 0.0) or 0.0),
            float(payload.get("lon", 0.0) or 0.0),
            int(payload.get("uptime", 0) or 0),
            payload.get("gas_raw"),
            bool(payload.get("gas_alert", False)),
            str(payload.get("door_status", "UNKNOWN")),
            str(payload.get("valve_status", "UNKNOWN")),
            ml_prob,
            ml_agree,
            vibe_class
        )
        with ingest_lock:
            tb_telemetry_ingest_buffer.append(row)
    except Exception as e:
        print(f"[Save Telemetry Buffer Error] {e}")

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
                magnitude, measured_velocity_kms, triggering_nodes, seismic_details, description,
                group_id, nodes_count
            )
            VALUES ('LOCAL_CONSENSUS', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            alarm_payload["epicenter_lat"],
            alarm_payload["epicenter_lon"],
            alarm_payload["radius_km"],
            alarm_payload.get("magnitude", 0),
            alarm_payload.get("measured_velocity_kms", 0),
            nodes_json,
            details_json,
            alarm_payload["desc"],
            alarm_payload.get("group_id", "cluster_chiba"),
            alarm_payload.get("nodes_count", len(alarm_payload.get("triggering_nodes", [])))
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
    client.subscribe([(TOPIC_STATUS, 0), (TOPIC_EVENT, 0), (TOPIC_TELEMETRY, 0), (TOPIC_EXTERNAL, 0), (TOPIC_LOG, 0), (TOPIC_OFFLINE, 0), (TOPIC_ACTUATOR, 0)])
    print(f"[SERVER] Listening to {TOPIC_STATUS}, {TOPIC_EVENT}, {TOPIC_TELEMETRY}, {TOPIC_LOG}, {TOPIC_OFFLINE}, {TOPIC_ACTUATOR}...")

def on_message(client, userdata, msg):
    payload = None
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
    except Exception:
        pass

    # 1. LOG MESSAGE (Dari Remote Serial Monitor ESP32)
    if "/log" in msg.topic:
        node_id = (payload.get("node_id") if isinstance(payload, dict) else None) or msg.topic.split('/')[2]
        message = (payload.get("message") if isinstance(payload, dict) else None) or msg.payload.decode('utf-8', errors='ignore')
        if node_id and message:
            with ingest_lock:
                logs_ingest_buffer.append((datetime.now(timezone.utc), node_id, str(message)))
        return

    # 1.1 ACTUATOR EVENT LOG (Perubahan state valve, door, buzzer, led)
    if "/actuator" in msg.topic:
        node_id = (payload.get("node_id") if isinstance(payload, dict) else None) or msg.topic.split('/')[2]
        actuator = payload.get("actuator") if isinstance(payload, dict) else None
        state = payload.get("state") if isinstance(payload, dict) else None
        reason = (payload.get("reason", "") if isinstance(payload, dict) else "") or ""
        ts_val = payload.get("ts") if isinstance(payload, dict) else None
        if ts_val and float(ts_val) > 1000000:
            act_time = datetime.fromtimestamp(float(ts_val), tz=timezone.utc)
        else:
            act_time = datetime.now(timezone.utc)
        if node_id and actuator and state:
            with ingest_lock:
                actuator_ingest_buffer.append((act_time, node_id, actuator, state, reason))
        return

    if not isinstance(payload, dict):
        return

    # 2. UPSERT NODE REGISTRY (Dari Heartbeat / Status)
    if "/status" in msg.topic:
        # Fallback node_id dari topic jika payload tidak ada
        node_id = payload.get("node_id") or msg.topic.split('/')[2]
        lat = payload.get("lat")
        lon = payload.get("lon")
        
        group_id = payload.get("group_id", node_registry.get(node_id, {}).get("group_id", "cluster_chiba"))
        
        if node_id and lat is not None and lon is not None:
            node_registry[node_id] = {
                "lat": lat,
                "lon": lon,
                "group_id": group_id,
                "last_seen": time.time()
            }
            
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO tb_nodes (node_id, role, lat, lon, group_id, last_seen) 
                        VALUES (%s, 'sensor', %s, %s, %s, NOW())
                        ON CONFLICT (node_id) 
                        DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon, group_id = COALESCE(EXCLUDED.group_id, tb_nodes.group_id), last_seen = NOW();
                    """, (node_id, lat, lon, group_id))
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[DB Error] {e}")
                finally:
                    release_db_connection(conn)

        # Ingest ke sensor_status untuk Grafana
        status = payload.get("status", "online")
        pose = payload.get("pose", "unknown")
        tilt_angle = payload.get("tilt_angle", 0.0)
        sent_ts = payload.get("ts", payload.get("timestamp"))
        latency_ms = (time.time() - sent_ts) * 1000.0 if sent_ts else payload.get("latency_ms", 0.0)
        sensor_ok = payload.get("sensor_ok", True)
        fw_version = payload.get("fw_version", "UNKNOWN")
        ota_status = payload.get("ota_status", "IDLE")
        motion_detected = payload.get("motion_detected")

        with ingest_lock:
            status_ingest_buffer.append((
                datetime.now(timezone.utc), node_id, status, pose, tilt_angle, latency_ms, sensor_ok, fw_version, ota_status, motion_detected
            ))
            logs_ingest_buffer.append((
                datetime.now(timezone.utc), node_id, f"Heartbeat: status={status}, pose={pose}, tilt={tilt_angle}°, fw={fw_version}"
            ))

    # 2. REKAMAN GEMPA OFFLINE (Store-and-Forward dari Flash Storage LittleFS ESP32)
    elif "/offline_events" in msg.topic:
        node_id = payload.get("node_id") or msg.topic.split('/')[2]
        pga = float(payload.get("pga", 0.0) or 0.0)
        sta_lta = float(payload.get("sta_lta", 0.0) or 0.0)
        freq_hz = int(payload.get("freq", payload.get("freq_hz", 0)) or 0)
        rms = float(payload.get("rms", 0.0) or 0.0)
        ax = float(payload.get("ax", 0.0) or 0.0)
        ay = float(payload.get("ay", 0.0) or 0.0)
        az = float(payload.get("az", 0.0) or 0.0)
        ml_prob = float(payload.get("ml", payload.get("ml_prob", 0.0)) or 0.0)
        ml_agree = bool(payload.get("agree", payload.get("ml_agree", True)))
        is_earthquake = bool(payload.get("quake", True))
        ts_val = payload.get("ts", time.time())
        uptime_ms = payload.get("uptime", 0)

        # Simpan ke tb_sensor_telemetry
        offline_payload = {
            "node_id": node_id,
            "ts": ts_val,
            "pga": pga,
            "sta_lta": sta_lta,
            "freq_hz": freq_hz,
            "rms": rms,
            "ax": ax,
            "ay": ay,
            "az": az,
            "uptime_ms": uptime_ms,
            "ml_prob": ml_prob,
            "ml_agree": ml_agree,
            "valve_status": "OFFLINE_RECORD",
            "door_status": "OFFLINE_RECORD"
        }
        save_telemetry(offline_payload)

        with ingest_lock:
            logs_ingest_buffer.append((
                datetime.now(timezone.utc), node_id,
                f"[OFFLINE RECOVERY] Gempa tersimpan di flash berhasil disinkronkan: PGA={pga:.4f}g, STA/LTA={sta_lta:.2f}, Freq={freq_hz}Hz, ML={ml_prob*100:.1f}%"
            ))

        if is_earthquake:
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO tb_sensor_events (time_event, node_id, pga, sta_lta, freq_hz, is_confirmed)
                        VALUES (to_timestamp(%s), %s, %s, %s, %s, true);
                    """, (ts_val if ts_val > 1000000 else time.time(), node_id, pga, sta_lta, freq_hz))
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[DB Offline Event Error] {e}")
                finally:
                    release_db_connection(conn)

        print(f"[OFFLINE RECOVERY] Rekaman gempa offline dari {node_id} tersimpan di DB: PGA={pga:.4f}g, ML={ml_prob*100:.1f}%")
        return

    # 3. EVENT / TELEMETRI (Ada Getaran dari Node)
    elif "/telemetry" in msg.topic:
        node_id = payload.get("node_id") or msg.topic.split('/')[2]
        
        # [DATA QUALITY GUARD] Validasi & Sanitasi Data Sensor
        try:
            pga = float(payload.get("pga", 0.0) or 0.0)
            if math.isnan(pga) or math.isinf(pga) or pga < 0.0:
                pga = 0.0
            elif pga > 3.0:
                pga = 3.0
        except (ValueError, TypeError):
            pga = 0.0
        payload["pga"] = pga

        try:
            sta_lta = float(payload.get("sta_lta", 1.0) or 1.0)
            if math.isnan(sta_lta) or math.isinf(sta_lta) or sta_lta < 0.0:
                sta_lta = 1.0
            elif sta_lta > 50.0:
                sta_lta = 50.0
        except (ValueError, TypeError):
            sta_lta = 1.0
        payload["sta_lta"] = sta_lta

        rms = payload.get("rms", sta_lta)
        ax = payload.get("ax", payload.get("dyn_x", 0.0))
        ay = payload.get("ay", payload.get("dyn_y", 0.0))
        az = payload.get("az", payload.get("dyn_z", 0.0))
        temp = payload.get("temperature")
        pres = payload.get("pressure")
        hum = payload.get("humidity")
        valve = payload.get("valve_status", "UNKNOWN")
        gas_raw = payload.get("gas_raw")
        gas_alert = payload.get("gas_alert")
        
        sent_ts = payload.get("ts", payload.get("timestamp"))
        try:
            sent_ts = float(sent_ts)
            now_server = time.time()
            if sent_ts < (now_server - 86400) or sent_ts > (now_server + 300):
                sent_ts = now_server
        except (ValueError, TypeError):
            sent_ts = time.time()
        payload["ts"] = sent_ts
        latency_ms = (time.time() - sent_ts) * 1000.0

        freq_hz = payload.get("freq_hz", 0)

        # TinyML Shadow Mode Evaluation
        if "ml_prob" in payload and payload["ml_prob"] is not None:
            ml_prob = float(payload.get("ml_prob", 0.0) or 0.0)
            ml_agree = bool(payload.get("ml_agree", True))
        else:
            # Fallback ke Server-side TinyML inference (sama persis dengan TinyML di C++ ESP32)
            # Menghitung probabilitas gempa real-time untuk firmware node v1.4.95 yang belum flashing OTA
            ml_prob = predict_tinyml(pga, sta_lta, freq_hz, rms)
            is_classic_quake = (pga >= 0.12 and (freq_hz == 0 or (0.5 <= freq_hz <= 8.0)))
            ml_agree = ((ml_prob >= 0.5) == is_classic_quake)

        payload["ml_prob"] = ml_prob
        payload["ml_agree"] = ml_agree

        vibe_class = str(payload.get("vibe_class") or "AMBIENT_TENANG")
        payload["vibe_class"] = vibe_class

        # Ingest semua telemetri ke sensor_telemetry untuk Grafana real-time monitor
        with ingest_lock:
            telemetry_ingest_buffer.append((
                datetime.now(timezone.utc), node_id, pga, rms, ax, ay, az, temp, pres, hum, latency_ms, valve, gas_raw, gas_alert, ml_prob, ml_agree, vibe_class
            ))
        # Simpan SETIAP pesan telemetri ke database (rekaman detik-per-detik tb_sensor_telemetry)
        # Termasuk data gas, cuaca, dll. sebelum filter membuang getaran lokal.
        save_telemetry(payload)

        # [ALGORITMA ANTI-HOAKS & FILTER SEISMIK MULTI-TIER]
        # Frekuensi seismik gempa tektonik riil berada pada 0.5 - 15.0 Hz.
        # Getaran mekanis meja, ketukan keyboard, langkah kaki, dan fan dominan di frekuensi tinggi (16 - 35 Hz).
        is_seismic_freq = (freq_hz == 0 or (0.5 <= freq_hz <= 15.0))
        
        # 1. Gempa Kuat / Terasa Nyata (PGA >= 0.05g):
        #    Harus berfrekuensi seismik (bukan benturan lokal keras frekuensi tinggi)
        if pga >= 0.05:
            is_real_quake = is_seismic_freq
            if not is_real_quake:
                print(f"[FILTER] Benturan lokal keras dari {node_id} ditolak (PGA:{pga:.3f}G, Freq:{freq_hz}Hz > 15Hz -> Hentakan/Benturan)")
        
        # 2. Gempa Sedang (0.020g <= PGA < 0.05g):
        #    Harus berfrekuensi seismik DAN lonjakan STA/LTA awal
        elif pga >= 0.020:
            is_real_quake = is_seismic_freq and (sta_lta >= 1.20 or ml_prob >= 0.40)
            
        # 3. Gempa Mikro / Ringan (0.0070g <= PGA < 0.020g, Shindo 1-2):
        #    WAJIB memiliki onset tajam (STA/LTA >= 1.50) atau konfirmasi TinyML (>= 0.50) dan frekuensi seismik murni (<= 15 Hz).
        #    Noise statis meja/ketukan keyboard (< 0.0070g atau STA/LTA ~ 0.8 - 1.3) DITOLAK 100%.
        elif pga >= 0.0070:
            is_real_quake = is_seismic_freq and (sta_lta >= 1.50 or ml_prob >= 0.50)
            
        else:
            is_real_quake = False

        if not is_real_quake:
            return # Buang noise meja, ketukan, langkah kaki, dan tremor statis non-seismik
        
        # Simpan ke tb_sensor_events untuk setiap kandidat getaran gempa yang lolos filter
        group_id = payload.get("group_id") or node_registry.get(node_id, {}).get("group_id", "cluster_chiba")
        conn_evt = get_db_connection()
        if conn_evt:
            try:
                cur_evt = conn_evt.cursor()
                ts_evt = payload.get("ts", time.time())
                cur_evt.execute("""
                    INSERT INTO tb_sensor_events (time_event, node_id, pga, sta_lta, freq_hz, group_id, is_confirmed)
                    VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, false);
                """, (ts_evt if ts_evt > 1000000 else time.time(), node_id, pga, sta_lta, freq_hz, group_id))
                conn_evt.commit()
                cur_evt.close()
            except Exception as e:
                print(f"[DB Sensor Event Error] {e}")
            finally:
                release_db_connection(conn_evt)
        
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
            
        print(f"[TELE] {node_id} | PGA:{pga:.3f}G | STA/LTA:{sta_lta:.1f} | {payload.get('freq_hz',0)}Hz | Group:{group_id}")
        
        new_trigger = {
            "node_id": node_id,
            "timestamp": payload.get("ts") or time.time(), # FIX: Gunakan epoch sensor
            "lat": lat,
            "lon": lon,
            "pga": pga,
            "group_id": group_id
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
            
            # Refine active earthquake if within REFINEMENT_WINDOW_SEC (60 seconds)
            if active_quake and (time.time() - active_quake["start_time"]) <= REFINEMENT_WINDOW_SEC:
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
                    mqtt_client.publish("lindu/actuator/cmd/all", json.dumps(update_payload))
                    mqtt_client.publish("lindu/sensor/cmd/all", json.dumps(update_payload))
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
            SELECT time_alert, epi_lat, epi_lon, radius_km, description, magnitude, measured_velocity_kms, triggering_nodes, group_id, nodes_count 
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
                "triggering_nodes": r[7] or [],
                "group_id": r[8] or "cluster_chiba",
                "nodes_count": r[9] or 2
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
    
    # 0. JIKA MASIH DALAM JENDELA COOLDOWN GEMPA:
    # Seluruh getaran selama 60 detik berasal dari gempa aktif yang sama.
    # Jangan buat alert baru di tb_system_alerts; telemetri otomatis dialihkan ke Live Refinement.
    if (now - last_alarm_time) < EVENT_COOLDOWN_SEC:
        trigger_buffer.clear()
        return

    # Hapus trigger yang usianya lebih dari 5.0 detik
    trigger_buffer = [t for t in trigger_buffer if (now - t["timestamp"]) <= 5.0]
    
    # Himpun node unik (simpan trigger dengan pga tertinggi jika ada duplikat dalam 5s)
    unique_nodes = {}
    for t in trigger_buffer:
        nid = t["node_id"]
        if nid not in unique_nodes or t["pga"] > unique_nodes[nid]["pga"]:
            unique_nodes[nid] = t
            
    if len(unique_nodes) < 2:
        return
        
    import itertools
    
    # Kelompokkan node berdasarkan group_id
    groups = {}
    for t in unique_nodes.values():
        gid = t.get("group_id", "cluster_chiba")
        if gid not in groups:
            groups[gid] = []
        groups[gid].append(t)

    # 1. EVALUASI KONSENSUS DALAM GRUP (INTRA-GROUP DYNAMIC QUORUM ADAPTIF)
    now_ts = time.time()
    for gid, g_nodes in groups.items():
        total_registered = sum(1 for n in node_registry.values() if n.get("group_id") == gid)
        
        # Hitung berapa node dalam grup ini yang sedang aktif / online (heartbeat <= 60s)
        active_online_in_group = sum(
            1 for n in node_registry.values() 
            if n.get("group_id") == gid and (now_ts - n.get("last_seen", 0) <= 60)
        )
        effective_count = active_online_in_group if active_online_in_group >= 2 else total_registered

        # ATURAN SKALABILITAS OTOMATIS:
        # 1. Jika grup hanya memiliki 2 node aktif   -> Quorum = 2 Node (100% grup kecil)
        # 2. Jika grup memiliki 3 node aktif         -> Quorum = 3 Node (100% Tri-Node, anti sentuhan meja)
        # 3. Jika grup memiliki >= 4 node aktif       -> Quorum = max(3, ceil(N * 0.67)) (Toleransi node lagging)
        if effective_count <= 2:
            required_quorum = 2
        elif effective_count == 3:
            required_quorum = 3
        else:
            required_quorum = max(3, math.ceil(effective_count * 0.67))

        if len(g_nodes) >= required_quorum:
            t_min = min(n["timestamp"] for n in g_nodes)
            t_max = max(n["timestamp"] for n in g_nodes)
            selisih_waktu = t_max - t_min
            
            # Hitung jarak terjauh antar pasangan dalam grup
            max_jarak = max(haversine_km(n1["lat"], n1["lon"], n2["lat"], n2["lon"]) 
                            for n1, n2 in itertools.combinations(g_nodes, 2)) if len(g_nodes) > 1 else 0.1
            
            kecepatan = max_jarak / selisih_waktu if selisih_waktu > 0.05 else 6.0
            print(f"\n[+] KONSENSUS GRUP '{gid}' TERCAPAI! {len(g_nodes)}/{total_registered} Node serentak bergetar (Syarat Quorum: {required_quorum} Node)!")
            print(f"    Nodes: {[n['node_id'] for n in g_nodes]} | Δt: {selisih_waktu:.2f}s | Max Dist: {max_jarak:.2f} km")
            
            fire_alarm(mqtt_client, g_nodes, gid, kecepatan, selisih_waktu)
            return
        else:
            print(f"[-] KONSENSUS GRUP '{gid}' BELUM TERCAPAI: Baru {len(g_nodes)} node aktif. Syarat grup '{gid}' ({total_registered} node terdaftar) adalah minimal {required_quorum} node!")

    # 2. FALLBACK CROSS-GROUP PAIRWISE (Jika node berasal dari grup berbeda)
    all_nodes = list(unique_nodes.values())
    for t1, t2 in itertools.combinations(all_nodes, 2):
        if t1.get("group_id") == t2.get("group_id"):
            continue
        max_pga = max(t1.get("pga", 0), t2.get("pga", 0))
        if max_pga < 0.05:
            continue
        jarak_km = haversine_km(t1["lat"], t1["lon"], t2["lat"], t2["lon"])
        selisih_waktu = abs(t2["timestamp"] - t1["timestamp"])
        kecepatan = 6.0 if selisih_waktu == 0 else (jarak_km / selisih_waktu)
        if (selisih_waktu == 0) or (1.5 <= kecepatan <= 12.0) or (selisih_waktu <= 0.5 and jarak_km < 3.0):
            print(f"[+] KONSENSUS LINTAS-GRUP TERCAPAI! {t1['node_id']} & {t2['node_id']}")
            fire_alarm(mqtt_client, [t1, t2], "cross_group", kecepatan, selisih_waktu)
            return

    print(f"[-] VALIDASI GAGAL! Tidak ada kecocokan fisika seismik dari {len(unique_nodes)} node.")

def fire_alarm(client, triggering_nodes, group_id, velocity, time_diff, epi_lat=None, epi_lon=None):
    global active_quake, last_alarm_time
    
    # COOLDOWN (DEDUPLIKASI GEMPA): Jika alarm sudah pernah dipicu < EVENT_COOLDOWN_SEC lalu,
    # JANGAN membuat alarm baru di database. Biarkan Live Refinement yang bekerja.
    if time.time() - last_alarm_time < EVENT_COOLDOWN_SEC:
        print(f"[COOLDOWN] Alarm diabaikan. Gempa ini masih dalam jendela pemurnian {int(EVENT_COOLDOWN_SEC)} detik.")
        return
    
    last_alarm_time = time.time()
    nodes_count = len(triggering_nodes)

    # Weighted Barycenter untuk Episentrum
    if epi_lat is None or epi_lon is None:
        total_pga = sum(max(0.0001, n.get("pga", 0.001)) for n in triggering_nodes)
        epi_lat = sum(n["lat"] * max(0.0001, n.get("pga", 0.001)) for n in triggering_nodes) / total_pga
        epi_lon = sum(n["lon"] * max(0.0001, n.get("pga", 0.001)) for n in triggering_nodes) / total_pga

    avg_pga = sum(n.get("pga", 0.001) for n in triggering_nodes) / nodes_count
    max_pga = max(n.get("pga", 0.001) for n in triggering_nodes)
    safe_pga = max(0.001, avg_pga)

    magnitude = round(4.0 + 1.5 * math.log10(safe_pga / 0.008), 1)
    magnitude = max(2.5, min(9.5, magnitude))
    dynamic_radius = round(10 ** (0.5 * magnitude - 1.0), 1)

    is_critical = (avg_pga >= 0.05 or max_pga >= 0.08)
    alarm_level = "CRITICAL" if is_critical else "ADVISORY"
    alarm_cmd = "trigger_siren" if is_critical else "quake_advisory"

    confidence = 100 if nodes_count >= 3 else 95

    node_details = [
        {"id": n["node_id"], "lat": n["lat"], "lon": n["lon"], "pga": n.get("pga", 0)}
        for n in triggering_nodes
    ]
    node_str = ", ".join([f"{n['node_id']} ({n.get('pga',0):.4f}G)" for n in triggering_nodes])

    if nodes_count >= 3:
        desc = f"{'🚨 Peringatan Darurat Gempa Kuat!' if is_critical else 'ℹ️ Gempa Terdeteksi (Advisory)'} [TRI-NODE QUORUM Grup {group_id}] {nodes_count} node serentak terpicu ({node_str}) dlm Δt={time_diff:.2f}s. Kecepatan P-Wave: {velocity:.1f} km/s."
    else:
        desc = f"{'🚨 Peringatan Darurat Gempa Kuat!' if is_critical else 'ℹ️ Gempa Terdeteksi (Advisory)'} [KONSENSUS PAIRWISE Grup {group_id}] Sensor {node_str} dlm Δt={time_diff:.2f}s. Kecepatan P-Wave: {velocity:.1f} km/s."

    alarm_payload = {
        "cmd": alarm_cmd,
        "level": alarm_level,
        "group_id": group_id,
        "nodes_count": nodes_count,
        "epicenter_lat": epi_lat,
        "epicenter_lon": epi_lon,
        "radius_km": dynamic_radius,
        "confidence": confidence,
        "magnitude": magnitude,
        "measured_velocity_kms": round(velocity, 2),
        "time_diff_s": round(time_diff, 2),
        "triggering_nodes": node_details,
        "desc": desc
    }

    active_quake = {
        'start_time': time.time(),
        'initial_lat': epi_lat,
        'initial_lon': epi_lon,
        'telemetry_count': 0,
        'group_id': group_id,
        'nodes_pga': {n['node_id']: n.get('pga', 0.1) for n in triggering_nodes},
        'nodes_coords': {n['node_id']: (n['lat'], n['lon']) for n in triggering_nodes}
    }

    print("\n=======================================================")
    print(f"🚨 KONSENSUS TERCAPAI! GEMPA M {magnitude} [{alarm_level}] ({nodes_count} NODES / GROUP {group_id}) 🚨")
    print(json.dumps(alarm_payload, indent=2))
    print("=======================================================\n")

    # Broadcast ke sistem
    client.publish(TOPIC_ALARM, json.dumps(alarm_payload))
    client.publish("lindu/sensor/cmd/all", json.dumps(alarm_payload))
    if is_critical:
        client.publish("lindu/actuator/cmd/all", json.dumps(alarm_payload))

    # Simpan ke database (Black Box)
    save_alert(alarm_payload)

    # Tandai event sensor sebagai terkonfirmasi (confirmed)
    try:
        conn_conf = get_db_connection()
        if conn_conf:
            cur_conf = conn_conf.cursor()
            node_ids = [n["node_id"] for n in triggering_nodes]
            cur_conf.execute("""
                UPDATE tb_sensor_events 
                SET is_confirmed = true 
                WHERE time_event >= NOW() - INTERVAL '30 seconds'
                  AND node_id = ANY(%s);
            """, (node_ids,))
            conn_conf.commit()
            cur_conf.close()
            release_db_connection(conn_conf)
    except Exception as e:
        print("[DB Confirm Event Error]", e)

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
            # Broadcast ke semua kemungkinan topik agar diterima oleh firmware versi lama maupun baru
            mqtt_client.publish(TOPIC_ALARM, json.dumps(payload))
            mqtt_client.publish("lindu/actuator/cmd/all", json.dumps(payload))
            mqtt_client.publish("lindu/sensor/cmd/all", json.dumps(payload))
            mqtt_client.publish(f"lindu/sensor/cmd/{target}", json.dumps(payload))
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
            WHERE time >= NOW() - INTERVAL '15 minutes'
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
        cur.execute("SELECT node_id, lat, lon, COALESCE(group_id, 'cluster_chiba'), last_seen FROM tb_nodes;")
        rows = cur.fetchall()
        
        nodes = []
        for r in rows:
            nodes.append({
                "node_id": r[0],
                "lat": r[1],
                "lon": r[2],
                "group_id": r[3],
                "last_seen": r[4].strftime("%Y-%m-%d %H:%M:%S") if r[4] else None
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


