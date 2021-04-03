
# import libraries we are going to use

import pandas as pd
import numpy as np
from pandas.io.json import json_normalize #package for flattening json into pandas df
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_sites_dataframe

# create a function that we will call in Tableau Prep

def get_calculated_fields(input):
        # set credentials to log in to your Tableau Server / Tableau Online. I'm using a personal token
        tableau_server_auth = {
                'dev-environment': {
                        'server': 'https://10ax.online.tableau.com/',  # update for your server
                        'api_version': '3.11',                         # update for your version of the API
                        'personal_access_token_name': 'PAT-NAME',      # update with your personal access token's name
                        'personal_access_token_secret': 'PAT-SECRET',  # update with your personal access token's secret
                        'site_name': "",                               # set it to '' if accessing your default site
                        'site_url': 'YOUR-SITE-CONTENT-URL'            # set it to '' if accessing your default site
                }
        }

        # log in to the server. If the connection was successful, we get 200 code
        connection = TableauServerConnection(tableau_server_auth, env='dev-environment')
        connection.sign_in()

        # get info about my server
        my_server = connection.server_info()
        print(my_server.json())

        # GraphQL query to get a list of calculations and dashboards where these calculations are used
        query_dashboards = """"
        query dataDictionary_dashboards {
        calculatedFields {
        id
        name
        description
        dataType
        formula
        downstreamDashboards {
        id
        name
        path
        }
        }
        }
        """
        # GraphQL query to get a list of workbooks for every dashboard
        query_workbooks = """"
        query dataDictionary_workbooks {
        dashboards {
        id
        workbook {
        name
        projectName
        updatedAt
        owner {
                username
                email
        }
        }
        }
        }
        """
        # GraphQL query to get a list of datasources and dashboards connected to them
        query_datasources = """"
        query dataDictionary_datasources {
        datasources {
        id
        name
        __typename
        upstreamDatabases {
        name
        __typename
        connectionType
        dataQualityWarnings{
                warningType
                authorDisplayName
                updatedAt
        }
        }
        downstreamDashboards {
        id
        }
        }
        }
        """
        
        # sending the query_dashboards to the Metadata API 
        response_dashboards = connection.metadata_graphql_query(query=query_dashboards)
        # The server sends us an HTTP response containing our query results in JSON format
        response_dashboards = response_dashboards.json()['data']['calculatedFields']

        # print the response
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Response in JSON:")
        print(response_dashboards)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")


        # converting json response to a Pandas dataframe              
        dashboards_normalized=pd.json_normalize(
                response_dashboards, 
                record_path=['downstreamDashboards'],
                meta=['id','name','description','dataType','formula'],record_prefix='Dashboard ', 
                meta_prefix='Calculation ')

        # print the data frame        
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Table with the list of calculated fields and related dashboards:")
        print(dashboards_normalized)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        # sending the query_workbooks to the Metadata API
        response_workbooks = connection.metadata_graphql_query(query=query_workbooks)
        # The server sends us an HTTP response containing our query results in JSON format
        response_workbooks = response_workbooks.json()['data']['dashboards']

        # print the response
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Response in JSON:")
        print(response_workbooks)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")


        # converting json response to normalised dataframe
        workbooks_normalized=pd.json_normalize(response_workbooks)
        workbooks_normalized.rename(columns={"id":"Dashboard id"},inplace=True)

        # print the data frame
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Table with the list of workbooks and related dashboards:")
        print(workbooks_normalized)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        # join workbooks and dashboards based on the Dashboard id field 
        workbooks_dashboards_df = pd.merge(left = dashboards_normalized, right = workbooks_normalized, how = 'left', left_on='Dashboard id', right_on='Dashboard id')

        # print the data frame
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Table with the list of calculated fields, workbooks, and dashboards:")
        print(workbooks_dashboards_df)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")


        # sending the query_datasources to the Metadata API 
        response_datasources = connection.metadata_graphql_query(query=query_datasources)
        # The server sends us an HTTP response containing our query results in JSON format
        response_datasources = response_datasources.json()['data']['datasources']

        # print the response
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Response in JSON:")
        print(response_datasources)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        # converting json response to normalised dataframe

        #first, get data on Upstream databases for each data source
        datasources_data = pd.json_normalize(response_datasources,
                                                record_path=['upstreamDatabases'], 
                                                meta=['id','name','__typename'], 
                                                record_prefix='Upstream Database ', 
                                                meta_prefix='Datasource ',
                                                errors='ignore')

        # print the data frame
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Table with the list of data sources:")
        print(datasources_data)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        #next, get data on dashboards for each data source
        dashboards_data = pd.json_normalize(response_datasources,
                                                record_path=['downstreamDashboards'], 
                                                meta=['id'], 
                                                record_prefix='Dashboard ', 
                                                meta_prefix='Datasource ',errors='ignore')
        
        # print the data frame                                        
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Table with the list of data sources and related dashboards:")
        print(dashboards_data)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        # then get the data on data qualityt warnings
        dataQualityWarnings_data = pd.json_normalize(response_datasources,
                                                record_path=['upstreamDatabases','dataQualityWarnings'], 
                                                meta=['id'], 
                                                record_prefix='Upstream Database Data Quality Warning ', 
                                                meta_prefix='Datasource ',
                                                errors='ignore')
        # print the data frame                                        
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Table with the list of data qualityt warnings:")
        print(dataQualityWarnings_data)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        #last, joining these 3 tables based on the Datasource id field 

        # first, datasources and dashboards on datasource id field

        datasources_dashboards_df = pd.merge(
        left = datasources_data, 
        right = dashboards_data, 
        how = 'outer', 
        left_on='Datasource id', 
        right_on='Datasource id')

        datasources_dashboards_df.drop(columns=['Upstream Database dataQualityWarnings'],inplace=True)

        # print the data frame 
        #print(datasources_dashboards_df)

        # and lastly adding data qualityt warnings data to this table, as well on datasource id field

        datasources_combined_df = pd.merge(
        left = datasources_dashboards_df, 
        right = dataQualityWarnings_data, 
        how = 'left', 
        left_on='Datasource id', 
        right_on='Datasource id')

        # print the data frame 
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Table with the combined list of data sources:")
        print(datasources_combined_df)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        # joining datasources_combined_df and workbooks_dashboards_df on Dashboard id field

        combined_df = pd.merge(
        left = datasources_combined_df, 
        right = workbooks_dashboards_df, 
        how = 'outer', 
        left_on='Dashboard id', 
        right_on='Dashboard id')

        # print the data frame 
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        print("Combined final table:")
        print(combined_df)
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

        # replace NaN values with 'None'

        combined_df = combined_df.replace(np.nan, '', regex=True)

        # renaming columns before outputting them to Tableau Prep

        combined_df.rename(
        columns={"Upstream Database __typename":"Upstream Database type",
                "Upstream Database connectionType":"Upstream Database connection type",
                "Datasource __typename":"Datasource type",
                "Upstream Database Data Quality Warning warningType": "Upstream Database Data Quality Warning type",
                "Upstream Database Data Quality Warning authorDisplayName": "Upstream Database Data Quality Warning author",
                "Upstream Database Data Quality Warning updatedAt": "Upstream Database Data Quality Warning last updated",
                "Calculation dataType": "Calculation data type",
                "workbook.name": "Workbook name",
                "workbook.projectName": "Workbook project",
                "workbook.updatedAt": "Workbook last updated",
                "workbook.owner.username": "Workbook owner"},
                inplace=True)

                     
        # converting all columns to strings

        all_columns = list(combined_df) # Creates list of all column headers
        combined_df[all_columns] = combined_df[all_columns].astype("str")

        # sign out of the server

        connection.sign_out()

        return combined_df  

# set the structure for data set coming to Tableau Prep

def get_output_schema():
	return pd.DataFrame({
		"Upstream Database name":prep_string(),
		"Upstream Database type":prep_string(),
		"Upstream Database connection type":prep_string(),
		"Datasource id":prep_string(),
		"Datasource name":prep_string(),
		"Datasource type":prep_string(),
		"Dashboard id":prep_string(),
		"Upstream Database Data Quality Warning type":prep_string(),
		"Upstream Database Data Quality Warning author":prep_string(),
		"Upstream Database Data Quality Warning last updated":prep_string(),
		"Dashboard name":prep_string(),
		"Dashboard path":prep_string(),
		"Calculation id":prep_string(),
		"Calculation name":prep_string(),
		"Calculation description":prep_string(),
		"Calculation data type":prep_string(),
                "Calculation formula":prep_string(),
                "Workbook name":prep_string(),
                "Workbook project":prep_string(),
                "Workbook last updated":prep_string(),
                "Workbook owner":prep_string()
	})        