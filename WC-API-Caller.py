#########################################################################################################################
#                                                                                                                       #
#       Wiki Contributions fetching script                                                                              #
#       This script fetches Wiki Contributions from the xtools user API,                                                #
#       and returns The output pandas dataframe is then exported in csv format.                                         #
#                                                                                                                       #
#########################################################################################################################


# Import necessary libraries
import json
import requests
import pandas as pd
import numpy as np

# ---------------
# Import data
df = pd.read_csv('2020-AMWC-EditorSample - SG1.1.csv', delimiter = ",")

X = df.loc[:,"Wiki"]
Y = df.loc[:,"UserName"]

# declare cleaning controller
DFCleaner = 0

# declare start & end date
start_date = '2020-01-01'
end_date = '2020-12-31'

# ---------------
# Create dataframe for url wikiname & username specifier
specifierX = X.iloc[:] + '/' + Y.iloc[:] + '/0/' + start_date + '/' + end_date
specifierX = pd.DataFrame(specifierX)

# Create dataframe for Edit count end point url
ECount_endpoint = []
ECount_endpoint.extend(['https://xtools.wmflabs.org/api/user/simple_editcount/' for i in range(df.shape[0])])
ECount_endpoint = pd.DataFrame(ECount_endpoint)

Edit_Count_url = ECount_endpoint + specifierX

# ---------------
# Create dataframe for url wikiname & username specifier
specifierZ = X.iloc[:] + '/' + Y.iloc[:] + '/0/all/all/' + start_date + '/' + end_date
specifierZ = pd.DataFrame(specifierZ)

# Create dataframe for Edit count end point url
PCount_endpoint = []
PCount_endpoint.extend(['https://xtools.wmflabs.org/api/user/pages_count/' for i in range(df.shape[0])])
PCount_endpoint = pd.DataFrame(PCount_endpoint)

Page_Count_url = PCount_endpoint + specifierZ

# ---------------
# Define API endpoints for fetching edit count and page count
seq = np.arange(1, df.shape[0] + 1, 1)

# Set up a list of fetched contributions
Wiki_contr = []

for i in seq:
    Fetch_url1 = Edit_Count_url.iloc[i - 1:i].to_string(header=False, index=False)

    EditCount = requests.get(Fetch_url1).json()
    FUserName = EditCount['username']
    FDCount = EditCount['deleted_edit_count']
    FLCount = EditCount['live_edit_count']

    Wiki_contr.append([FUserName, FLCount, FDCount])

Wiki_contr = pd.DataFrame(Wiki_contr)
Wiki_contr.columns =['username', 'live_edit_count', 'deleted_edit_count']

# ---------------
# Define API endpoints for fetching edit count and page count
seq = np.arange(1, df.shape[0] + 1, 1)

# Set up a list of fetched contributions
Wiki_contr1 = []

for i in seq:
    Fetch_url2 = Page_Count_url.iloc[i - 1:i].to_string(header=False, index=False)
    PageCount_obj = requests.get(Fetch_url2).json()
    PageCount = pd.json_normalize(PageCount_obj)

    FPCountL = PageCount.iloc[0:1, 8:9]
    FPCountD = PageCount.iloc[0:1, 11:12]
    FPLength = PageCount.iloc[0:1, 10:11]

    Wiki_contr1.append([FPCountL, FPCountD, FPLength])

Wiki_contr1 = pd.DataFrame(Wiki_contr1)

# ---------------
# Make sure this is run only once
if DFCleaner == 0:
    # Clean Wiki_contr1 data frame
    Wiki_contr1.columns = ['Live Page Count', 'Deleted page count', 'Average page length']

    # Replace empty data frame
    Wiki_contr1['Live Page Count'] = Wiki_contr1['Live Page Count'].apply(
        lambda a: str(a).replace('Empty DataFrame\nColumns: []\nIndex: [0]', 'counts.count 0 0 0'))
    Wiki_contr1['Deleted page count'] = Wiki_contr1['Deleted page count'].apply(
        lambda a: str(a).replace('Empty DataFrame\nColumns: []\nIndex: [0]', 'counts.deleted 0 0 0'))
    Wiki_contr1['Average page length'] = Wiki_contr1['Average page length'].apply(
        lambda a: str(a).replace('Empty DataFrame\nColumns: []\nIndex: [0]', 'counts.avg_length 0 0 0 0'))

    # Delete unnecessary characters
    Wiki_contr1['Live Page Count'] = Wiki_contr1['Live Page Count'].map(lambda x: str(x)[17:])
    Wiki_contr1['Deleted page count'] = Wiki_contr1['Deleted page count'].map(lambda x: str(x)[19:])
    Wiki_contr1['Average page length'] = Wiki_contr1['Average page length'].map(lambda x: str(x)[23:])

    # Set cleaner flag
    DFCleaner = 1

# ---------------
# Join the two tables from the api calls
result = pd.concat([Wiki_contr, Wiki_contr1], axis=1, join='inner')
result.head()

#write the output to a csv file on the desktop
result.to_csv(r'/Users/dumisani/Desktop/WikiContributions-'+start_date+'-'+end_date+'.csv', index = False)

