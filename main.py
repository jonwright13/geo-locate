import warnings, json, os, requests, easygui, sqlite3
import pandas as pd
from time import sleep, time
from datetime import datetime

with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=pd.errors.SettingWithCopyWarning)

spacer_size = 100
rate_limit = 90000
csv_backup = "data/backup.csv"
json_backup = 'data/backup.json'

def get_original_data(URL):
    '''
    Method gets the original data from a database
        Params: URL (str) | Url for the databse
        Return: Dataframe
    '''
    print("Getting original data from database")

    db_connection = sqlite3.connect(URL)
    df = pd.read_sql_query("SELECT * FROM sightings", db_connection)
    print(f"Database collected. Size: {len(df)}")

    db_connection.close()
    print("Database connection closed\n")
    return df

def get_backup_data(df):
    '''
    Method gets the backup data from a csv (Most up to date with added countries) and parses a completed and
    incomplete database
        params: df (DataFrame) | Original Pandas dataframe
        Return: completed (Dataframe), incomplete (Dataframe)
    '''
    try:
        print("Getting backup data from csv")
        completed = pd.read_csv(csv_backup)
        incomplete = df[len(completed):]
    except:
        print("Backup unavailable. Creating dataset from database")
        completed = pd.DataFrame()
        incomplete = df.copy()
    
    
    print(f"Parsed compled / incomplete datasets | Completed: {len(completed)} | Incomplete: {len(incomplete)}")
    return completed[[col for col in completed.columns if "Unnamed" not in col]], incomplete

def locate(latitude, longitude):

    request = requests.get(f"https://geocode.maps.co/reverse?lat={latitude}&lon={longitude}")

    if request.status_code not in [401, 429, 503, 403]:
        return request.json()

    else:
        print(f"Received status Code: {request.status_code}")
        return False


def locate_loop(coords_list):
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
    country_list = []
    json_data = get_json()

    print("\n", "*" * spacer_size)
    print("\nBeginning to locate countries")

    for coord in coords_list:
        if usage <= rate_limit:
            lat = coord[1]
            lon = coord[2]
            print(lat, lon)

            location = locate(lat, lon)

            if not location:
                break
            else:

                json_data.append({"index": coord[0], "request": location})
        
                if 'error' not in location:
                    address = location['address']['country']
                    country_list.append(address)
                    print(f"Usage: {usage}/{rate_limit} | Index: [{coord[0]}/{len(coords_list)-1}] | Coords: {coord[1:]} | Address: {address}")
                else:
                    country_list.append('not_found')
                    print(f"Usage: {usage}/{rate_limit} | Index: [{coord[0]}/{len(coords_list)-1}] | Coords: {coord[1:]} | Address: None Found")

                sleep(0.5)


        else:
            print(f"Usage Limit Hit: {usage-1}/{rate_limit}")

            break

        usage += 1
    
    print("\n", "*" * spacer_size)

    return country_list, json_data


def add_countries(completed, incomplete, country_list):
    '''
    Appends the country list into the incomplete dataframe and then combines with the completed dataframe
    Only rows where country_fixed are combined into the completed dataframe
    Incomplete rows are kept within the incomplete dataframe
        Params:
            ► completed (Dataframe) | dataset where country_fixed already contains entries
            ► incomplete (Dataframe) | dataset missing entries for country_fixed
            ► country_list (List) | List of countries to combine with the incomplete dataframe
        Return:
            ► completed (Dataframe) | dataset where country_fixed already contains entries
            ► incomplete (Dataframe) | dataset missing entries for country_fixed
            ► country_list (List) | Blank list just in case
    '''

    print(f"\nAdding country list to data with list size: {len(country_list)}")
    print(f"Completed original size: {len(completed)} | Incomple original size: {len(incomplete)}")

    temp = incomplete.copy()

    # Extending country_list to the same size as the incomplete dataframe but all elems set to None
    country_list += [None] * (len(temp) - len(country_list))

    # Adding the country_list to the temp dataframe
    temp['Country_Fixed'] = country_list

    # Re-constructing new completed/incomplete dataframes
    completed = pd.concat([completed, temp.loc[~temp['Country_Fixed'].isna()]])
    incomplete = temp.loc[temp['Country_Fixed'].isna()]

    print(f"Completed new size: {len(completed)} | Incomple new size: {len(incomplete)}")
    print("\n", "*" * spacer_size, "\n")
    return completed, incomplete, []



def export_backup(completed):
    '''
    Saves the completed dataframe to a csv as a backup
        Params: completed (Dataframe) | Dataset where country_fixed already contains entries
        Returns: None
    '''
    completed.to_csv(csv_backup)
    print("Backup successfully saved")

def export_json(data):
    '''
    Exports the json data to a file
        Params: data (List) | List of dicts
        Return: None
    '''
    with open('data/geo_data.json', "w") as json_file:
        json.dump(data, json_file)

    print("JSON data exported")

def get_json():
    
    print("\nRetrieving JSON data")

    if os.path.exists(json_backup):

        print("JSON data exists. Appending to existing")

        with open('data/geo_data.json', "r") as json_file:
            data = json.load(json_file)
    else:
        print("JSON data does not exist. Starting from scratch")
        data = []
    return data

def export_data(completed, json_data):
    export_backup(completed)
    export_json(json_data)

def main():
    '''
    Main script. Runs through all the functions above in order
        1. Get original data from database
        2. Get backup data
        3. Search for and compile a list of countries
        4. Re-construct a new dataset containing only entries that have a country associated with them
        5. Save to a csv
    Params: None
    Return: None
    '''

    URL = easygui.fileopenbox(title="Select Database", filetypes=["*.db"])

    start = time()
    print(f"\nBeginning script at {datetime.now()}\n")
    
    df = get_original_data(URL)
    completed, incomplete = get_backup_data(df)
    coords_list = list(zip(incomplete.index, incomplete['latitude'], incomplete['longitude']))
    country_list, json_data = locate_loop(coords_list)
    completed, incomplete, country_list = add_countries(completed, incomplete, country_list)
    export_data(completed, json_data)

    print("Script Complete")

    duration = time() - start
    print(f"\nDuration: {duration/60:.2f} mins at {datetime.now()}\n")
    

if __name__ == '__main__':
    main()