import pandas as pd
import requests
import aiohttp
import asyncio
import functions.census_functions as census

class census_tract():
    def __init__(self, year, profile, state_id, api_key):
        """ 
        year = year of desired census api
        profile = data profile type desired from census api
        state_id = state desired from census api
        api_key = api key needed for census api
        """

        self.year = year
        self.profile = profile 
        self.state_id = state_id
        self.api_key = api_key

    def census_tract_api(self):
        """
        Output: 
            Returns raw census api data as pandas dataframe
        """
        
        url = f"https://api.census.gov/data/{self.year}/acs/acs5/profile?get=group({self.profile})&for=tract:*&in=state:{self.state_id}&key={self.api_key}"

        # request url for provided variable code & year
        get_response = requests.get(url)

        # convert api return as into json
        json_return = get_response.json()

        # convert json to pandas data frame
        df_tract = pd.DataFrame(json_return)  
        
        return df_tract
    
    def census_variable_names(self, variables):
        """
        Input: 
            variables = list of variables provided for the desire census profile

        Output: 
            Returns census api variable & variable names
        """

        url = "https://api.census.gov/data/2020/acs/acs5/profile/variables/{}.json"

        variable_labels = []

        async def get_variables():
            async with aiohttp.ClientSession() as session:
                for var in variables:
                    response = await session.get(url.format(var), ssl=False)
                    json_return = await response.json()
                    variable_label = json_return["label"]
                    variable_labels.append(variable_label)
        asyncio.run(get_variables())

        return variable_labels
    
    def apply_variable_cols(self):
        """
        Output: 
            Returns dataframe with census variable names as column headers
        """
        
        df_tract = self.census_tract_api()

        # transpose first row containing all variable codes
        df_vars = df_tract.iloc[:1, :].T.rename(columns={0:"variable"})

        # columns to keep
        col_keep = list(df_vars.variable[-5:])

        # get variable names from census codes
        variable_names = self.census_variable_names(list(df_vars.variable[:-5]))

        # combine columns
        new_cols = variable_names + col_keep

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

        # create data frame of geographic data
        df_census_geo = df.iloc[:, -5:]

        # replace header with first row as lowercase strings
        df_census_geo.columns = [ col.lower() for col in df_census_geo.iloc[0] ] 

        df_census_geo = df_census_geo[1:]

        return df_census_geo
    
    def geo_variables(self, df):

        """
        Input: 
            df = dataframe constructed from census api (output from apply_variable_cols())

        Output: 
            Returns dataframe of re-organized geo variables
        """

        # get variables for each geo id
        geo_df_lst = []
        df_census_variables = df.iloc[1:, :-4] # remove additional geo data

        for geo_id in df_census_variables["GEO_ID"]:
            df_census_variables_filter = df_census_variables[df_census_variables["GEO_ID"] == geo_id] # filter for each geo_id; every row of the dataframe
            geo_dict = df_census_variables_filter.set_index("GEO_ID").agg(dict,1).to_dict() # create dictionary of geo_id and each of it's variables + values
            geo_dict_values = list(geo_dict.values())[0]
            initial_geo_df = pd.DataFrame({"variable_name": list(geo_dict_values.keys()), "value": list(geo_dict_values.values())}) # create temp dataframe of the geo's variables & values
            initial_geo_df["geo_id"] = list(geo_dict.keys())[0] # create column so every row has the corresponding geo_id
            initial_geo_df["value"] = initial_geo_df["value"].fillna(0) 
            geo_df_lst.append(initial_geo_df)

        geo_variables_df = pd.concat(geo_df_lst)

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

        return geo_variable_codes
    
    def create_census_tract_df(self):
        """
        Output: 
            Collects all census API data and returns the final dataframe after cleaning & re-organizing variables
        """

        census_api_df = self.apply_variable_cols()

        geo_df = self.geo_df(census_api_df)

        variables_df = self.variable_categories(census_api_df)

        census_tract_df = geo_df.merge(variables_df, on = "geo_id", how = "left")

        return census_tract_df


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int, help="The year you want census data for")
    parser.add_argument("profile", type=str, help="The data profile you want census data for")
    parser.add_argument("state_id", type=str, help="The state you want census data for")
    args = parser.parse_args()

    census_class = census_tract(args.year, args.profile, args.state_id, census.census_key())
    df_final = census_class.create_census_tract_df()

    df_final.to_csv(f"census_tract_output_state{args.state_id}.csv", index = False)



