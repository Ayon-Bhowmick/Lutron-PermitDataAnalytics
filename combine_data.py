import pandas as pd # pip install pandas
import os

# list constants that hold the identified similiar names
LIST_ISSUE_DATE = {"issue_date", "job_start_date", "issued_date", "permitissuedate"}
LIST_CONTRACTOR = {"contractor_company_name", "contact_1_name","firm_name", "contractor_name", "contractorname"}
LIST_LATITUDE =  {"latitude","gis_latitude", "lat"}
LIST_LONGITUDE = {"longitude","gis_longitude", "lng"}

# dataframe to store the combined data of all the cities after standardization
combined_df = pd.DataFrame()

# pointing towards the directory with our stripped data
os.chdir("./stripped_data")

# for loop that will append the data to the lists
for file in os.listdir(os.getcwd()):
    # dataframe to store the standarized data for each city
    standardized_df = pd.DataFrame()
    # read all files that end with .csv
    if file.endswith('.csv'):
        city = pd.read_csv(file)
        # for loop that checks for similiar attribute names and standardizes it
        for i in LIST_ISSUE_DATE:
            # if a similiar attribute name from LIST_ISSUE_DATE is in city.columns, then standardize it
            if(i in city.columns):
                standardized_df['issued_date'] = pd.Series(city[i])
        # if a similiar attribute name from LIST_CONTRACTOR is in city.columns, then standardize it
        for i in LIST_CONTRACTOR:
            if(i in city.columns):
                standardized_df['contractor'] = pd.Series(city[i]) 
        # if a similiar attribute name from LIST_LATITUDE is in city.columns, then standardize it        
        for i in LIST_LATITUDE:
            if(i in city.columns):
                standardized_df['latitude'] = pd.Series(city[i])
        # if a similiar attribute name from LIST_LONGITUDE is in city.columns, then standardize it     
        for i in LIST_LONGITUDE:
            if(i in city.columns):
                standardized_df['longitude'] = pd.Series(city[i])
    
        # get file name using os.path.basename(file)
        file_name = os.path.basename(file)
        # get the file name which is the first index in the format file_name.csv if we are splitting by '.' 
        city_name = file_name.split('.')[0]
        standardized_df['city'] = city_name
        # list to hold the city and the combined data frame for concat
        combine = [combined_df, standardized_df]
        # concat the city data frame and the current combined data frame
        combined_df = pd.concat(combine)
        
# create csv with the dataframe with the combined data and store it in the combined_data folder
combined_df.to_csv('../combined_data/combinedData.csv')
        