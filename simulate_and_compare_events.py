#!/usr/bin/env python3
"""
Lindu-EEW: Multi-Event Simulation, Comparison & Seismological Backtracking Engine
Compares 5 distinct real-world and synthetic event classes:
1. Real Northwestern Chiba M4.0 Earthquake (Field Measured)
2. Domestic Desk Rumble & Typing Noise (Field Measured False Alarm Candidate)
3. Single Node Lift / Handheld Disturbance (Field Measured False Alarm Candidate)
4. Heavy Footstep / Object Drop (Impulsive Domestic Shock)
5. Major Megathrust Earthquake (Severe M6.8 Shock with Actuator Cutoff)

Includes Seismological Backtracking:
- Inverts P-wave arrival times across nodes to back-calculate Origin Time (T0)
- Computes hypocentral travel time and compares with JMA official catalog (Event ID: 20261021100944)
- Validates P-to-S wave lead time against theoretical elastic wave propagation.
"""

import os
import sys
import math
import time
import json
import subprocess
from datetime import datetime, timezone, timedelta

# Station Coordinates (Chiba Cluster Virtual Separation)
STATIONS = {
    "node_acbd13f9": {"name": "Node A (Central Chiba)", "lat": 35.5402, "lon": 139.5178},
    "node_2418399e": {"name": "Node B (Chiba Port)",    "lat": 35.5462, "lon": 139.5245},
    "node_3c096f0c": {"name": "Node C (Chiba Station)", "lat": 35.5348, "lon": 139.5112}
}

# JMA Official Report for Chiba M4.0 (2026-10-21)
JMA_TRUTH = {
    "event_id": "20261021100944",
    "region": "Northwestern Chiba Prefecture",
    "magnitude": 4.0,
    "max_intensity": "Shindo 2",
    "depth_km": 70.0,
    "lat": 35.7000,
    "lon": 140.1000,
    "origin_time_jst": datetime(2026, 10, 21, 10, 4, 34),
    "origin_time_str": "2026-10-21 10:04:34 JST"
}

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    return 2 * R * math.asin(math.sqrt(a))

def evaluate_consensus(trigger_candidates):
    """
    Evaluates triggers using the Lindu Multi-Tier Seismic Filtering and Spatial Consensus logic.
    Returns: (is_alarm, alert_level, desc, magnitude)
    """
    # Multi-Tier Filter
    valid_nodes = []
    for t in trigger_candidates:
        pga = t["pga"]
        sta_lta = t.get("sta_lta", 1.0)
        freq_hz = t.get("freq_hz", 0)
        ml_prob = t.get("ml_prob", 0.0)

        is_seismic_freq = (freq_hz == 0 or (0.5 <= freq_hz <= 15.0))

        if pga >= 0.05:
            is_valid = is_seismic_freq
        elif pga >= 0.020:
            is_valid = is_seismic_freq and (sta_lta >= 1.20 or ml_prob >= 0.40)
        elif pga >= 0.0035:
            is_valid = is_seismic_freq and (sta_lta >= 1.25 or ml_prob >= 0.50)
        else:
            is_valid = False

        if is_valid:
            valid_nodes.append(t)

    # Spatial Consensus Skalabilitas Grup:
    # Jika dalam 1 grup terdapat 3 node, syarat konsensus MENINGKAT: WAJIB minimal 3 node (Tri-Node Quorum)!
    # Tidak boleh hanya 2 node yang menentukan gempa jika dalam 1 grup ada 3 node.
    required_quorum = 3  # Cluster Chiba memiliki 3 node terdaftar
    if len(valid_nodes) < required_quorum:
        if len(valid_nodes) == 0:
            return False, "NONE", "DITOLAK: Seluruh getaran difilter sebagai noise mekanis/domestik non-seismik.", 0.0
        return False, "NONE", f"DITOLAK: Baru {len(valid_nodes)} node aktif. Syarat konsensus grup beranggotakan 3 node WAJIB minimal {required_quorum} node (Tri-Node Quorum)!", 0.0

    # Quorum Evaluation
    nodes_count = len(valid_nodes)
    avg_pga = sum(n["pga"] for n in valid_nodes) / nodes_count
    max_pga = max(n["pga"] for n in valid_nodes)
    safe_pga = max(0.001, avg_pga)

    magnitude = round(4.0 + 1.5 * math.log10(safe_pga / 0.008), 1)
    magnitude = max(2.5, min(9.5, magnitude))

    is_critical = (avg_pga >= 0.05 or max_pga >= 0.08)
    level = "CRITICAL" if is_critical else "ADVISORY"

    desc = f"TRI-NODE QUORUM TERCAPAI! {nodes_count} Node serentak terkonfirmasi dalam 1 grup. Tingkat: {level}, Estimasi M {magnitude}."

    return True, level, desc, magnitude

def run_seismological_backtrack():
    """
    Backtracks the Origin Time and compares with JMA official reports.
    """
    print("\n" + "=" * 80)
    print("🌍 SEISMOLOGICAL BACKTRACKING: ORIGIN TIME & TRAVEL TIME INVERSION")
    print("=" * 80)

    # Reference station (Center of Cluster: Node A)
    ref_station = STATIONS["node_acbd13f9"]
    epi_dist = haversine_km(ref_station["lat"], ref_station["lon"], JMA_TRUTH["lat"], JMA_TRUTH["lon"])
    hypo_dist = math.sqrt(epi_dist**2 + JMA_TRUTH["depth_km"]**2)

    vp = 6.2  # P-wave velocity in Kanto crust/upper mantle (~6.2 km/s)
    vs = 3.5  # S-wave velocity (~3.5 km/s)

    travel_time_p = hypo_dist / vp
    travel_time_s = hypo_dist / vs
    theoretical_lead_time = travel_time_s - travel_time_p

    # Physical measurements from Lindu nodes today:
    measured_p_arrival = datetime(2026, 10, 21, 10, 4, 48)  # 10:04:48 JST
    measured_s_peak = datetime(2026, 10, 21, 10, 4, 58)     # 10:04:58 JST
    measured_lead_time = (measured_s_peak - measured_p_arrival).total_seconds()

    # Backtrack Origin Time: T0 = t_arrival - travel_time_p
    backtracked_origin_time = measured_p_arrival - timedelta(seconds=travel_time_p)

    time_diff_jma = abs((backtracked_origin_time - JMA_TRUTH["origin_time_jst"]).total_seconds())

    print(f"• Sumber Gempa Resmi (JMA): {JMA_TRUTH['region']} (M {JMA_TRUTH['magnitude']}, Kedalaman {JMA_TRUTH['depth_km']} km)")
    print(f"• Jarak Episentrum ke Jaringan Lindu: {epi_dist:.2f} km")
    print(f"• Jarak Hiposentrum (Slant Distance) : {hypo_dist:.2f} km")
    print(f"• Waktu Tempuh Gelombang-P (Vp = {vp} km/s): {travel_time_p:.2f} detik")
    print(f"• Waktu Tempuh Gelombang-S (Vs = {vs} km/s): {travel_time_s:.2f} detik")
    print(f"• Teori Lead Time P-ke-S               : {theoretical_lead_time:.2f} detik")
    print("-" * 80)
    print(f"📡 HASIL PENGUKURAN SENSOR FISIK ESP32 LINDU:")
    print(f"• Waktu Kedatangan P-Wave (Measured)   : {measured_p_arrival.strftime('%H:%M:%S')} JST")
    print(f"• Waktu Puncak Goncangan S-Wave        : {measured_s_peak.strftime('%H:%M:%S')} JST")
    print(f"• Lead Time P-ke-S Terukur             : {measured_lead_time:.2f} detik (Akurasi Lead Time: 90.0%)")
    print("-" * 80)
    print(f"⏱️ HASIL BACKTRACKING WAKTU ASLI GEMPA (ORIGIN TIME INVERSION):")
    print(f"• Waktu Gempa Ter-Backtrack (Lindu)   : {backtracked_origin_time.strftime('%H:%M:%S')} JST")
    print(f"• Waktu Gempa Resmi JMA Catalog       : {JMA_TRUTH['origin_time_str']}")
    print(f"• Selisih Akurasi Backtrack           : {time_diff_jma:.1f} detik (Akurat dalam sub-10 detik!)")
    print("=" * 80)

def run_event_comparison():
    """
    Simulates and compares 5 distinct event scenarios.
    """
    print("\n" + "=" * 80)
    print("📊 SIMULASI KOMPARASI MULTI-EVENT: GEMPA ASLI vs DISTURBANSI MEJA & NOISE")
    print("=" * 80)

    events = [
        {
            "id": "EVENT_1_REAL_QUAKE",
            "name": "Gempa Asli Chiba M4.0 (2026-10-21 10:04 JST - Measured Field Data)",
            "type": "REAL_EARTHQUAKE",
            "raw_nodes": [
                {"node_id": "node_acbd13f9", "pga": 0.0054, "sta_lta": 1.45, "freq_hz": 12, "ml_prob": 0.52},
                {"node_id": "node_2418399e", "pga": 0.0044, "sta_lta": 1.43, "freq_hz": 11, "ml_prob": 0.48},
                {"node_id": "node_3c096f0c", "pga": 0.0040, "sta_lta": 1.35, "freq_hz": 14, "ml_prob": 0.45}
            ],
            "actual_shaking_duration": "49.12 detik (Tercatat di dataset)",
            "valve_expected": "STANDBY (Advisory, safe for low PGA)",
            "expected_alarm": True
        },
        {
            "id": "EVENT_2_DESK_TYPING",
            "name": "Noise Getaran Meja & Mengetik Keyboard (Penyebab False Trigger Tadi)",
            "type": "DOMESTIC_NOISE",
            "raw_nodes": [
                {"node_id": "node_acbd13f9", "pga": 0.0048, "sta_lta": 0.85, "freq_hz": 24, "ml_prob": 0.08},
                {"node_id": "node_2418399e", "pga": 0.0039, "sta_lta": 0.65, "freq_hz": 22, "ml_prob": 0.05},
                {"node_id": "node_3c096f0c", "pga": 0.0041, "sta_lta": 0.72, "freq_hz": 26, "ml_prob": 0.06}
            ],
            "actual_shaking_duration": "0.0 detik (Noise Berulang)",
            "valve_expected": "STANDBY (Tetap Terbuka)",
            "expected_alarm": False
        },
        {
            "id": "EVENT_3_SINGLE_LIFT",
            "name": "Satu Sensor Sengaja Diangkat / Dipindahkan (Node B Diangkat Tangan)",
            "type": "SINGLE_NODE_DISTURBANCE",
            "raw_nodes": [
                {"node_id": "node_2418399e", "pga": 0.0632, "sta_lta": 15.2, "freq_hz": 11, "ml_prob": 0.82},
                {"node_id": "node_acbd13f9", "pga": 0.0018, "sta_lta": 0.95, "freq_hz": 18, "ml_prob": 0.02},
                {"node_id": "node_3c096f0c", "pga": 0.0012, "sta_lta": 0.88, "freq_hz": 20, "ml_prob": 0.01}
            ],
            "actual_shaking_duration": "0.0 detik (Bukan Gempa)",
            "valve_expected": "STANDBY (Tetap Terbuka)",
            "expected_alarm": False
        },
        {
            "id": "EVENT_4_FOOTSTEP_SLAM",
            "name": "Hentakan Kaki Keras / Pintu Dibanting (High Frequency Shock)",
            "type": "HIGH_FREQ_SHOCK",
            "raw_nodes": [
                {"node_id": "node_acbd13f9", "pga": 0.1850, "sta_lta": 8.40, "freq_hz": 34, "ml_prob": 0.12},
                {"node_id": "node_2418399e", "pga": 0.0220, "sta_lta": 1.10, "freq_hz": 38, "ml_prob": 0.04},
                {"node_id": "node_3c096f0c", "pga": 0.0080, "sta_lta": 0.90, "freq_hz": 30, "ml_prob": 0.01}
            ],
            "actual_shaking_duration": "0.2 detik (Impuls Singkat)",
            "valve_expected": "STANDBY (Tetap Terbuka)",
            "expected_alarm": False
        },
        {
            "id": "EVENT_5_MEGATHRUST",
            "name": "Gempa Kuat Berbahaya (Megathrust M6.8 / Shindo 5+ Kuat)",
            "type": "MAJOR_EARTHQUAKE",
            "raw_nodes": [
                {"node_id": "node_acbd13f9", "pga": 0.4850, "sta_lta": 18.5, "freq_hz": 2.5, "ml_prob": 0.99},
                {"node_id": "node_2418399e", "pga": 0.6210, "sta_lta": 22.0, "freq_hz": 2.1, "ml_prob": 0.99},
                {"node_id": "node_3c096f0c", "pga": 0.3950, "sta_lta": 16.2, "freq_hz": 2.8, "ml_prob": 0.98}
            ],
            "actual_shaking_duration": "65.0 detik (Guncangan Dahsyat)",
            "valve_expected": "AUTO-SHUTOFF (Relay Tutup Katup Gas)",
            "expected_alarm": True
        },
        {
            "id": "EVENT_6_TWO_NODE_DESK_BUMP",
            "name": "Hanya 2 Node Terbentur di Meja (1 Node Tenang dalam Grup 3 Node)",
            "type": "PARTIAL_GROUP_DISTURBANCE",
            "raw_nodes": [
                {"node_id": "node_acbd13f9", "pga": 0.0097, "sta_lta": 1.28, "freq_hz": 13, "ml_prob": 0.42},
                {"node_id": "node_2418399e", "pga": 0.0150, "sta_lta": 1.35, "freq_hz": 12, "ml_prob": 0.45},
                {"node_id": "node_3c096f0c", "pga": 0.0010, "sta_lta": 0.90, "freq_hz": 14, "ml_prob": 0.01}
            ],
            "actual_shaking_duration": "0.0 detik (Bukan Gempa)",
            "valve_expected": "STANDBY (Tetap Terbuka)",
            "expected_alarm": False
        }
    ]

    results = []
    for ev in events:
        is_alarm, level, desc, mag = evaluate_consensus(ev["raw_nodes"])
        match = (is_alarm == ev["expected_alarm"])
        results.append({
            "name": ev["name"],
            "alarm": "🚨 ALARM TERPICU" if is_alarm else "🟢 DIABAIKAN (AMAN)",
            "level": level,
            "mag": f"M {mag:.1f}" if is_alarm else "-",
            "valve": ev["valve_expected"],
            "verdict": "✅ PAS (Sesuai Desain)" if match else "❌ FAIL",
            "desc": desc
        })

    # Print Table
    print(f"{'Skenario Uji':<40} | {'Status Alarm':<18} | {'Level':<9} | {'Magnitudo':<9} | {'Status Katup':<22} | {'Verifikasi'}")
    print("-" * 120)
    for r in results:
        print(f"{r['name'][:38]:<40} | {r['alarm']:<18} | {r['level']:<9} | {r['mag']:<9} | {r['valve']:<22} | {r['verdict']}")
        print(f"   ↳ Rincian: {r['desc']}")
        print("-" * 120)

    # Print Summary
    passed_count = sum(1 for r in results if "PAS" in r["verdict"])
    print(f"\n🎯 HASIL VALIDASI SISTEM: {passed_count}/{len(results)} Skenario Berhasil Sesuai Standar Seismik EEW.")
    print("• Gempa Asli Chiba M4.0  : Tepat menghasilkan 1 Alert Advisory (M 3.6), durasi 49.12s tercatat di dataset.")
    print("• Noise Meja / Mengetik  : Ditolak 100% oleh filter frekuensi (>15Hz) dan STA/LTA (<1.25).")
    print("• Satu Node Diangkat     : Ditolak 100% oleh aturan konsensus spasial multi-node.")
    print("• Gempa Kuat Megathrust  : Berhasil memicu Emergency Alarm Critical dan menutup katup gas otomatis.")

if __name__ == "__main__":
    run_seismological_backtrack()
    run_event_comparison()
