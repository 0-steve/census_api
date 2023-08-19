# census_api analysis

## Goal #1

Call census api for social characteristics data for all recognized states. Clean the data to create a human readable dataframe. The script _census_tract_api_ can be run from terminal with the following parameters:

```
year = year of desired census api (integer)
profile = data profile type desired from census api (string)

output = csv file of final census dataframe
```

Run the function _get_state_codes_ to get a csv of all state codes from census. Make sure to place the csv file output in the folder _state_codes_. The script will use this file to collect census tract data for all included states.

## Goal #2
Once applied to all eligible states, conduct further analysis to find trends and statistical summaries on social characteristics data.

## Goal #3
Conduct clustering analysis on on social characteristics data for all eligible states & census tracts.

## References
```
https://www.census.gov/data/developers/data-sets/acs-5year.html
https://api.census.gov/data/2021/acs/acs5/profile/groups/DP02.html
```

## Coming soon

Further documentation into census.gov API & required python packages


