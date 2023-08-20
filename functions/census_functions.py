import os
import configparser
import requests
import pandas as pd

def census_key():
    """
    Output: 
        Returns your Census API key
    """
    
    secret_path = os.path.expanduser("~") + "/.secrets" # .secrets should live in your home directory 
    config = configparser.RawConfigParser()
    config.read(secret_path)

    details_dict = dict(config.items("CENSUS_API"))
    airtable_key = details_dict["census_api_key"]

    return airtable_key

def get_state_codes():
    """
    Output: 
        Census state codes & state names as a csv file
    """

    api_key = census_key()

    print("")
    print("Retrieving state codes from Census ...")

    url = f"https://api.census.gov/data/2010/dec/sf1?get=NAME&for=state:*&key={api_key}"

    # request url for provided variable code & year
    get_response = requests.get(url)

    # convert api return as into json
    json_return = get_response.json()

    # convert api return into dataframe & rename header
    df_state_code = pd.DataFrame(json_return[1:-1], dtype = "str").rename(columns = {0: "name", 1: "state_code"})

    print("")
    print("State codes output as csv file")

    df_state_code.to_csv("census_state_codes.csv", index = False)