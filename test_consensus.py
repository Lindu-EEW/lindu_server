import unittest
import math
from consensus import haversine_km

class TestConsensusEngine(unittest.TestCase):
    def test_haversine_distance(self):
        lat_jkt, lon_jkt = -6.2088, 106.8456
        lat_bdg, lon_bdg = -6.9175, 107.6191
        dist = haversine_km(lat_jkt, lon_jkt, lat_bdg, lon_bdg)
        self.assertTrue(110 < dist < 125, f"Jarak JKT-BDG salah: {dist} km")

        lat_tky, lon_tky = 35.6895, 139.6917
        lat_yok, lon_yok = 35.4437, 139.6380
        dist2 = haversine_km(lat_tky, lon_tky, lat_yok, lon_yok)
        self.assertTrue(25 < dist2 < 35, f"Jarak TKY-YOK salah: {dist2} km")

        self.assertEqual(haversine_km(lat_jkt, lon_jkt, lat_jkt, lon_jkt), 0.0)

    def calculate_center_of_energy(self, nodes):
        total_pga = sum(n['pga'] for n in nodes)
        if total_pga == 0:
            return nodes[0]['lat'], nodes[0]['lon']
        epi_lat = sum(n['lat'] * n['pga'] for n in nodes) / total_pga
        epi_lon = sum(n['lon'] * n['pga'] for n in nodes) / total_pga
        return epi_lat, epi_lon

    def test_center_of_energy(self):
        nodes = [
            {"id": "A", "lat": 10.0, "lon": 20.0, "pga": 1.0},
            {"id": "B", "lat": 20.0, "lon": 20.0, "pga": 1.0}
        ]
        lat, lon = self.calculate_center_of_energy(nodes)
        self.assertEqual(lat, 15.0)
        self.assertEqual(lon, 20.0)

        nodes_weighted = [
            {"id": "A", "lat": 10.0, "lon": 20.0, "pga": 3.0},
            {"id": "B", "lat": 20.0, "lon": 20.0, "pga": 1.0}
        ]
        lat_w, lon_w = self.calculate_center_of_energy(nodes_weighted)
        self.assertEqual(lat_w, 12.5)
        self.assertEqual(lon_w, 20.0)

    def validate_p_wave(self, dist_km, time_diff_s):
        if time_diff_s <= 0: return False, 0
        velocity = dist_km / time_diff_s
        return (1.0 <= velocity <= 15.0), velocity

    def test_p_wave_validation(self):
        is_valid, v = self.validate_p_wave(30, 5)
        self.assertTrue(is_valid)
        self.assertEqual(v, 6.0)
        
        is_valid, v = self.validate_p_wave(10, 20)
        self.assertFalse(is_valid)
        
        is_valid, v = self.validate_p_wave(100, 1)
        self.assertFalse(is_valid)

if __name__ == '__main__':
    unittest.main()
