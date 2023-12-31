import pandas as pd
import numpy as np
import os
import dask.dataframe as dd 
import aiohttp
import asyncio
from functools import lru_cache
from datetime import datetime

import sys
sys.path.insert(0, "../functions/")
import census_functions as census

class census_tract():
    def __init__(self, year, profile, state_codes, api_key):
        """ 
        year = year of desired census api
        profile = data profile type desired from census api
        state_codes = tuple of desired states from census api
        api_key = api key needed for census api
        """

        self.year = year
        self.profile = profile 
        self.state_codes = state_codes
        self.api_key = api_key

    def census_tract_api(self):
        """
        Output: 
            Returns raw census api data as pandas dataframe
        """

        census_tract_data = []

        print("")
        print("Connecting to Census API ...")

        async def get_census_tract_data():
            session_timeout = aiohttp.ClientTimeout(total=None)
            async with aiohttp.ClientSession(timeout=session_timeout) as session:
                for code in self.state_codes:
                    response = await session.get(f"https://api.census.gov/data/{self.year}/acs/acs5/profile?get=group({self.profile})&for=tract:*&in=state:{code}&key={self.api_key}", ssl=False)
                    json_return = await response.json()
                    census_tract_data.append(json_return)
                    print("Found census data for state code:", code)

        asyncio.run(get_census_tract_data())

        print("Found all states!")

        return census_tract_data
    
    @lru_cache(maxsize=128)
    def census_variable_names(self, variables, year=2020):
        """
        Input: 
            variables = tuple of variables provided for the desire census profile

        Output: 
            Returns census api variable & variable names
        """

        url = f"https://api.census.gov/data/{year}/acs/acs5/profile/variables/" + "{}.json"

        variable_labels = []

        print("")
        print("Finding variable names from each variable code ...")

        async def get_variables():
            async with aiohttp.ClientSession() as session:
                for var in variables:
                    response = await session.get(url.format(var), ssl=False)
                    json_return = await response.json()
                    variable_label = json_return["label"]
                    variable_labels.append(variable_label)
        asyncio.run(get_variables())

        variable_labels = np.array(variable_labels)

        print("Found all variables names!")

        return variable_labels
    
    @lru_cache(maxsize=128)
    def apply_variable_cols(self):
        """
        Output: 
            Returns dataframe with census variable names as column headers
        """
        
        census_tract_api_return = self.census_tract_api()

        df_tract = pd.concat([ pd.DataFrame(api_return) for api_return in census_tract_api_return ])

        # transpose first row containing all variable codes
        df_vars = df_tract.iloc[:1, :].T.rename(columns={0:"variable"})

        # columns to keep
        col_keep = np.array(df_vars.variable[-5:])

        # get variable names from census codes
        variable_names = self.census_variable_names(tuple(df_vars.variable[:-5]), self.year)

        # combine columns
        new_cols = np.concatenate((variable_names, col_keep))

        # apply column names
        df_tract.columns = new_cols
        
        return df_tract
    
    def geo_df(self, df):
        """
        Input: 
            df = dataframe constructed from census api (output from apply_variable_cols())

        Output: 
            Returns dataframe of only geographic data
        """
        print("")
        print("Creating dataframe for geography data ...")

        # create data frame of geographic data
        df_census_geo = df.iloc[:, -5:]

        # replace header with first row as lowercase strings
        df_census_geo.columns = [ col.lower() for col in df_census_geo.iloc[0] ] 

        df_census_geo = df_census_geo[df_census_geo.geo_id != "GEO_ID"]

        df_census_geo = df_census_geo.rename(columns = {"name": "tract_name", "state": "state_code"})

        print("Geography dataframe created!")

        return df_census_geo
    
    def geo_variables(self, df):

        """
        Input: 
            df = dataframe constructed from census api (output from apply_variable_cols())

        Output: 
            Returns dataframe of re-organized geo variables
        """

        print("")
        print("Cleaning variable names ...")

        # get variables for each geo id
        df_census_variables = df.iloc[1:, :-4] # remove additional geo data

        df_census_variables = df_census_variables[df_census_variables.loc[:, "GEO_ID"] != "GEO_ID"]

        geo_dict = df_census_variables.set_index("GEO_ID").agg(dict,1).to_dict()  # create dictionary of geo_id and each of it's variables + values

        geo_dict_values = np.array(list(geo_dict.values()))

        # geo_variables_df dataframe of the geo's variables & values
        geo_variables_df = pd.DataFrame({"variable_name": [ k for d in geo_dict_values for k in d.keys() ], "value": [ k for d in geo_dict_values for k in d.values() ]})

        geo_variables_df["geo_id"] = np.repeat(list(geo_dict.keys()), len(geo_variables_df["variable_name"].unique()))

        geo_variables_df["value"] = geo_variables_df["value"].fillna(0)

        # create column so every row has the corresponding geo_id
        variable_code_names = pd.DataFrame(np.vstack([df.columns, df])).iloc[:2, :-5].T.rename({0: "variable_name", 1: "variable_code"}, axis = 1)

        geo_variables_df = geo_variables_df.merge(variable_code_names, on = "variable_name", how = "left")

        print("Variable names cleaned!")

        return geo_variables_df
    
    def variable_categories(self, df):
        """
        Input: 
            df = dataframe constructed from census api (output from apply_variable_cols())

        Output: 
            Returns output dataframe from geo_variables() and creates variable categories
        """

        # break up variable name into three components: the measurement type, demographic_target, demographic
        # create a column for each component for df_variable_codes

        geo_variable_codes = self.geo_variables(df)

        print("")
        print("Breaking up variables by measurement type, demographic_target, & demographic ...")

        def breakout_measurement(variable_name):

            var_split = variable_name.split("!!")

            if len(var_split) == 0:
                return None

            col_measure = var_split[0]

            # measurement.append(col_measure.lower()) # measurement value
            measurement = col_measure.lower()

            return measurement

        def breakout_demographic_target(variable_name):

            var_split = variable_name.split("!!")

            if len(var_split) == 0:
                return None

            # demographic_target.append(var_split[1].lower()) # demographic target
            demographic_target = var_split[1].lower()

            return demographic_target

        def breakout_demographic(variable_name):

            var_split = variable_name.split("!!")

            if len(var_split) == 0:
                return None

            if len(var_split[2:]) > 1:
                # demographic.append(" ".join(map(str, var_split[2:])).lower()) # demographic
                demographic  = " ".join(map(str, var_split[2:])).lower()
            else: 
                # demographic.append(var_split[2].lower()) # demographic
                demographic  = var_split[2].lower()

            return demographic

        print("Breaking out measurement") # remove
        geo_variable_codes["measurement"] = geo_variable_codes["variable_name"].map(breakout_measurement)
        print("Breaking out demographic target") # remove
        geo_variable_codes["demographic_target"] = geo_variable_codes["variable_name"].map(breakout_demographic_target)
        print("Breaking out demographic") # remove
        geo_variable_codes["demographic"] = geo_variable_codes["variable_name"].map(breakout_demographic)

        print("Converting to dask") # remove
        # convert to dask
        geo_variable_codes = dd.from_pandas(geo_variable_codes, npartitions = 991)

        print("Variable breakouts created!")

        return geo_variable_codes
    
    def create_census_tract_df(self):
        """
        Output: 
            Collects all census API data and returns the final dataframe after cleaning & re-organizing variables
        """

        census_api_df = self.apply_variable_cols()

        geo_df = self.geo_df(census_api_df)

        variables_df = self.variable_categories(census_api_df)

        census_tract_df = variables_df.merge(geo_df, on = "geo_id", how = "left") #.merge(census_variable_codes, on = "variable_name", how = "left")

        return census_tract_df
    
    def final_census_tract_df(self, state_name_df):
        """
        Input: 
            df = dataframe constructed from create_census_tract_df()

        Output: 
            Returns dataframe with state names appended on
        """

        census_tract_df = self.create_census_tract_df()

        census_tract_df_final = census_tract_df.merge(state_name_df, on = "state_code").rename(columns = {"name": "state_name"})
    
        # re order columns
        cols = ["geo_id", "state_code", "state_name", "county", "tract", "tract_name", "variable_code", 
                "variable_name", "value", "measurement", "demographic_target", "demographic"]

        census_tract_df_final = census_tract_df_final[cols]

        print("")
        print("Census tract dataframe created")

        return census_tract_df_final


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int, help="The year you want census data for")
    parser.add_argument("profile", type=str, nargs='?', default="DP02", help="The data profile you want census data for")
    args = parser.parse_args()

    startTime = datetime.now()

    state_codes_df = pd.read_csv("../state_codes/census_state_codes.csv", dtype = "str")

    state_codes = tuple(state_codes_df["state_code"])

    print("")
    print(f"Finding census data for {len(state_codes)} states ...")

    census_class = census_tract(args.year, args.profile, state_codes, census.census_key())
    census_state_df = census_class.final_census_tract_df(state_codes_df)

    filename = f"census_tract_{args.year}.parquet"
    census_state_df.to_parquet(filename, write_index = False)

    print("")
    print(f"Census tract output {filename} saved in {os.getcwd()}")

    print("Run time", datetime.now() - startTime)