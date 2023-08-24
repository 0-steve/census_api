import pandas as pd
import os
import aiohttp
import asyncio
from functools import lru_cache
import functions.census_functions as census

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
    def census_variable_names(self, variables):
        """
        Input: 
            variables = tuple of variables provided for the desire census profile

        Output: 
            Returns census api variable & variable names
        """

        url = "https://api.census.gov/data/2020/acs/acs5/profile/variables/{}.json"

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
        col_keep = list(df_vars.variable[-5:])

        # get variable names from census codes
        variable_names = self.census_variable_names(tuple(df_vars.variable[:-5]))

        # combine columns
        new_cols = list(variable_names) + col_keep

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

        df_census_geo = df_census_geo[1:].rename(columns = {"name": "tract_name", "state": "state_code"})

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
        # geo_df_lst = []
        df_census_variables = df.iloc[1:, :-4] # remove additional geo data

        def clean_geo_variables(geo_id):
            df_census_variables_filter = df_census_variables[df_census_variables["GEO_ID"] == geo_id] # filter for each geo_id; every row of the dataframe
            geo_dict = df_census_variables_filter.set_index("GEO_ID").agg(dict,1).to_dict() # create dictionary of geo_id and each of it's variables + values
            geo_dict_values = list(geo_dict.values())[0]
            initial_geo_df = pd.DataFrame({"variable_name": list(geo_dict_values.keys()), "value": list(geo_dict_values.values())}) # create temp dataframe of the geo's variables & values
            initial_geo_df["geo_id"] = list(geo_dict.keys())[0] # create column so every row has the corresponding geo_id
            initial_geo_df["value"] = initial_geo_df["value"].fillna(0) 

            return initial_geo_df

        geo_df_lst = map(clean_geo_variables, df_census_variables["GEO_ID"])

        geo_variables_df = pd.concat(geo_df_lst)

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

        measurement = []
        demographic_target = []
        demographic = []

        for var in geo_variable_codes["variable_name"]:
            var_split = var.split("!!")

            if len(var_split) == 0:
                continue

            col_measure = var_split[0]

            measurement.append(col_measure.lower()) # measurement value
            demographic_target.append(var_split[1].lower()) # demographic target
            
            if len(var_split[2:]) > 1:
                demographic.append(" ".join(map(str, var_split[2:])).lower()) # demographic
            else: 
                demographic.append(var_split[2].lower()) # demographic

        geo_variable_codes["measurement"] = measurement
        geo_variable_codes["demographic_target"] = demographic_target
        geo_variable_codes["demographic"] = demographic

        print("Variable breakouts created!")

        return geo_variable_codes
    
    def create_census_tract_df(self):
        """
        Output: 
            Collects all census API data and returns the final dataframe after cleaning & re-organizing variables
        """

        census_api_df = self.apply_variable_cols()

        census_variable_codes = census_api_df.iloc[:1, :-5].T.reset_index().rename({"index": "variable_name", 0: "variable_code"}, axis = 1)

        geo_df = self.geo_df(census_api_df)

        variables_df = self.variable_categories(census_api_df)

        census_tract_df = geo_df.merge(variables_df, on = "geo_id", how = "left").merge(census_variable_codes, on = "variable_name", how = "left")

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
        cols = list(df.columns[:2]) + list(df.columns[-1:]) + list(df.columns[2:5]) + list(df.columns[-2:-1]) + list(df.columns[5:-2])

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

    state_codes_df = pd.read_csv("state_codes/census_state_codes.csv", dtype = "str")
    state_codes = tuple(state_codes_df["state_code"])

    print("")
    print(f"Finding census data for {len(state_codes)} states ...")

    census_class = census_tract(args.year, args.profile, state_codes, census.census_key())
    census_state_df = census_class.final_census_tract_df(state_codes_df)

    filename = f"census_tract_{args.year}.csv"
    census_state_df.to_csv(filename, index = False)

    print("")
    print(f"Census tract output {filename} saved in {os.getcwd()}")



