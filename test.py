import unittest
from main import locate, locate_loop

class TestLocation(unittest.TestCase):
    def test_single(self):
        latitude = 29.8830556
        longitude = -97.9411111
        location = locate(latitude, longitude)
        print(location)

        country = "United States"
        self.assertEqual(location['address']['country'], country)

    def test_multi(self):
        coords_list = [[0, 29.8830556,-97.9411111], [1, 29.38421,-98.581082], [2, 28.9783333,-96.6458333]]
        country_list, json_data = locate_loop(coords_list)
        self.assertEqual(len(country_list), 3)

if __name__ == "__main__":
    unittest.main()