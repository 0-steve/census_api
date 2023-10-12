# census_api analysis

## Goal #1 (1_build)

Call census api for social characteristics data for all recognized states (minus PR). Clean the data to create a human readable dataframe. The script _census_tract_api_ can be run from terminal with the following parameters:

```
year = year of desired census api (integer)
profile = data profile type desired from census api (string)
  -default is "DP02"

output = csv file of the state's cleaned census tract dataframe
  -all 51 state codes returns 103,998,049 rows, 12 columns
```

Run the function _get_state_codes_ to get a csv of all state codes (minus PR) from census. Make sure to place the csv file output in the folder _state_codes_. The script will use this file to collect census tract data for all included states.

## Goal #2 (2_learn)
Conduct exploratory data analysis on social characteristics data for each state returned from 1_build. Learn trends & visualize analysis in a Jupyter notebook.

![Alt text](/refs/mean_internet_users.png?raw=true)

![Alt text](/refs/histogram_internet.png?raw=true)

## Goal #3 (3_query)
Load data into DuckDB to run SQL queries for futher data transformation and analysis. Create an interactive chart with Altair to see ancestral country of origin by state.

![Alt text](/refs/ny_ancestry.png?raw=true)

## References
```
https://www.census.gov/data/developers/data-sets/acs-5year.html
https://api.census.gov/data/2021/acs/acs5/profile/groups/DP02.html
```


