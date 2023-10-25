import requests, json, os
from time import sleep, time
from datetime import datetime

# Geocoding API: https://geocode.maps.co/

spacer_size = 100

class Geo_Locate:

    '''
    Geo_Locate takes a list of latitude and longitude coordinates (Same size lists) and requests location data from the geocode.maps api
    Can export the location data to a JSON file within the current directory or can pull as a class attribute

    Params:
        latitude_list (str) | List of latitudes
        longitude_list (str) | List of latitudes
        limit (Bool) | Whether to enforce rate limiting. Default = False
        export (Bool) | Whether to export the JSON data after completion. Default = True
    '''

    rate_limit = 90000
    json_backup = 'data/geo_data.json'
    sleep_duration = 0.5

    def __init__(self, latitude_list=[], longitude_list=[], limit=False, export=True):

        start = time()
        print(f"\nBeginning script at {datetime.now()}\n")

        self.limit = limit
            
        coordinates = list(zip([i for i in range(0,len(latitude_list))], latitude_list, longitude_list))

        self.json_data = self.locate_country(coordinates)

        if export:
            self._export_json(self.json_data)

        duration = time() - start
        print(f"\nDuration: {duration/60:.2f} mins at {datetime.now()}\n")

    @staticmethod
    def _request_location(latitude, longitude):
        return requests.get(f"https://geocode.maps.co/reverse?lat={latitude}&lon={longitude}")
    
    def _print_output(self, usage, index, coords, message, coord_length):
        print(f"Usage: {usage}/{self.rate_limit} | Index: [{index}/{len(coord_length)-1} | Coords: {coords} | Address: {message}")

    def _export_json(data):
        '''
        Exports the json data to a file
            Params: data (List) | List of dicts
            Return: None
        '''
        with open('data/geo_data.json', "w") as json_file:
            json.dump(data, json_file)

        print("JSON data exported")

    def locate_country(self, coordinates):

        '''
        Takes the incomplete dataframe, extracts the index and coordinates from each row into a list of tuples
        Loops through the list and calls the geocode.maps api with the latitude and longitude to identify the
        country, which is appended to a list
        Errors are appended to the list with a none_found identifier to avoid indexing issues

        Params: incomplete (Dataframe) | Dataframe of the incomplete data
        Return: 
            ► country_list (List) | List of countries
            ► json_data (list) | list of json data
        '''
        
        usage = 1

        json_data = []
        coord_length = len(coordinates)

        print("\n", "*" * spacer_size)
        print("\nBeginning to locate countries")

        for coord in coordinates:
            
            if usage <= self.rate_limit:
            
                request = self._request_location(coord[1], coord[2])

                if request.status_code not in [429, 503, 403]:

                    location = request.json()

                    json_data.append({"index": coord[0], "request": location})
            
                    if 'error' not in location:
                        self._print_output(usage, coord[0], coord[1:], location['address']['country'], coord_length)
                    else:
                        self._print_output(usage, coord[0], coord[1:], 'None Found', coord_length)

                    sleep(self.sleep_duration)

                else:
                    print(f"Received status Code: {request.status_code}")
                    break

            else:
                print(f"Usage Limit Hit: {usage-1}/{self.rate_limit}")
                break

            if self.limit:
                usage += 1
        
        print("\n", "*" * spacer_size)

        return json_data

if __name__ == '__main__':
    Geo_Locate()