"""Processes the individual raw data files and combines them into a single csv file"""

import pandas as pd
import logging
# from awsglue.context import GlueContext
import sys
from numpy import nan
import boto3
from io import StringIO

RAW_BUCKET = "lehigh-permit-raw-data-bucket"
PROCESSED_BUCKET = "lehigh-permit-processed-data-bucket"
COMBINED_BUCKET = "lehigh-permit-combined-data-bucket"

# glueContext = GlueContext(sc)
# logger = glueContext.get_logger()

def read_s3(bucket, key) -> pd.DataFrame:
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'])

def write_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())


def austin() -> pd.DataFrame:
    """
    Extracts needed columns from the Austin data and saves it as a csv file
    :return austin_stripped: the stripped data as a pandas dataframe
    """
    # read in raw data from s3
    austin_raw = read_s3(RAW_BUCKET, "austin.csv")

    # only save rows with electrical contractor
    austin_stripped = austin_raw[austin_raw.contractor_trade == "Electrical Contractor"]

    # extract only the columns we need
    austin_stripped = austin_stripped[["issue_date", "location", "contractor_company_name", "electrical_valuation_remodel"]]

    # extract latitude and longitude from location column
    # {'latitude': '30.29406429', 'longitude': '-97.69323996', 'human_address': '{""address"": """", ""city"": """", ""state"": """", ""zip"": """"}'}
    austin_stripped["latitude"] = austin_stripped["location"].apply(lambda x: x.split(",")[0].split("'")[3] if type(x) == str else nan)
    austin_stripped["longitude"] = austin_stripped["location"].apply(lambda x: x.split(",")[1].split("'")[3] if type(x) == str else nan)

    # drop rows with missing contractor_company_name
    austin_stripped = austin_stripped.dropna(subset=["contractor_company_name"])
    austin_stripped.drop(columns=["location"], inplace=True)

    # split issue_date to only include date
    austin_stripped["issue_date"] = austin_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    write_s3(austin_stripped, PROCESSED_BUCKET, "austin.csv")
    print("Saved austin.csv")
    return austin_stripped

def new_york() -> pd.DataFrame:
    """
    Extracts needed columns from the New York data and saves it as a csv file
    :return new_york_stripped: the stripped data as a pandas dataframe
    """
    # read in raw data from s3
    new_york_raw = read_s3(RAW_BUCKET, "new_york.csv")

    # extract only the columns we need
    new_york_stripped = new_york_raw[["job_start_date", "firm_name", 'gis_latitude', 'gis_longitude']]

    # add a NULL column to match the other dataframes
    new_york_stripped["valuation"] = nan

    # drop rows with missing firm_name
    new_york_stripped = new_york_stripped.dropna(subset=["firm_name"])

    # split issue_date to only include date
    new_york_stripped["job_start_date"] = new_york_stripped["job_start_date"].apply(lambda x: x.split("T")[0] if type(x) == str else nan)
    write_s3(new_york_stripped, PROCESSED_BUCKET, "new_york.csv")
    print("Saved new_york.csv")
    return new_york_stripped

def chicago() -> pd.DataFrame:
    """
    Extracts needed columns from the Chicago data and saves it as a csv file
    :return: chicago_stripped: the stripped data as a pandas dataframe
    """
    # read in raw data from s3
    chicago_raw = read_s3(RAW_BUCKET, "chicago.csv")

    # copy the dataframe
    chicago_stripped = chicago_raw.copy()

    # remove non electrical contractors
    contractor_names = [f"contact_{x}_name" for x in range(1, 12)]
    contractor_types = [f"contact_{x}_type" for x in range(1, 12)]
    for contractor, t_col in zip(contractor_names, contractor_types):
        chicago_stripped[contractor] = chicago_stripped.apply(lambda row: row[contractor] if row[t_col] == "CONTRACTOR-ELECTRICAL" else nan, axis=1)

    # melt the dataframe to have one row per contractor
    chicago_stripped = pd.melt(chicago_stripped, id_vars=["issue_date", "latitude", "longitude", "reported_cost"], value_vars=contractor_names, var_name="num", value_name="contractor_name")

    # drop rows with missing contractor_name
    chicago_stripped = chicago_stripped.dropna(subset=["contractor_name"])

    #  extract only the columns we need
    chicago_stripped = chicago_stripped[["issue_date", "contractor_name", "latitude", "longitude", "reported_cost"]]

    # split issue_date to only include date
    chicago_stripped["issue_date"] = chicago_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    write_s3(chicago_stripped, PROCESSED_BUCKET, "chicago.csv")
    print("Saved chicago.csv")
    return chicago_stripped

def philly()-> pd.DataFrame:
    """
    Extracts needed columns from the Philadelphia data and saves it as a csv file
    :return: philly_stripped: the stripped data as a pandas dataframe
    """
    philly_raw = read_s3(RAW_BUCKET, "philly.csv")
    philly_stripped = philly_raw[philly_raw.permitdescription == "ELECTRICAL PERMIT"]
    philly_stripped = philly_stripped[["permitissuedate", "contractorname", "address", "lat", "lng"]]
    philly_stripped = philly_stripped.dropna(subset=["contractorname"])
    philly_stripped["permitissuedate"] = philly_stripped["permitissuedate"].apply(lambda x: x.split(" ")[0])

    # concat the market_value column to the philly_stripped dataframe from the second philly csv using lat lng as a shared key
    philly_raw2 = read_s3(RAW_BUCKET, "philly_valuation.csv")
    philly_raw2 = philly_raw2[["location", "market_value"]]
    # rename the column location to address to match the other philly csv
    philly_raw2.rename(columns={"location": "address"}, inplace=True)
    philly_stripped = pd.merge(philly_stripped, philly_raw2, on=["address"], how="left")
    write_s3(philly_stripped, PROCESSED_BUCKET, "philly.csv")
    print("Saved philly.csv")
    return philly_stripped

def mesa()-> pd.DataFrame:
    """
    Extracts needed columns from the Mesa data and saves it as a csv file
    Can't filter by contractor trade
    :return: mesa_stripped: the stripped data as a pandas dataframe
    """
    # read in raw data from s3
    mesa_raw = read_s3(RAW_BUCKET, "mesa.csv")

    # extract only the columns we need
    mesa_stripped = mesa_raw[["issued_date", "contractor_name", "latitude", "longitude", "value"]]

    # drop rows with missing contractor_name
    mesa_stripped = mesa_stripped.dropna(subset=["contractor_name"])

    # split issue_date to only include date
    mesa_stripped["issued_date"] = mesa_stripped["issued_date"].apply(lambda x: x.split("T")[0])
    write_s3(mesa_stripped, PROCESSED_BUCKET, "mesa.csv")
    print("Saved mesa.csv")
    return mesa_stripped

def la()-> pd.DataFrame:
    """
    Extracts needed columns from the Los Angeles data and saves it as a csv file
    :return: la_stripped: the stripped data as a pandas dataframe
    """
    # read in raw data from s3
    la_raw = read_s3(RAW_BUCKET, "la.csv")

    # extract only the columns we need
    la_stripped = la_raw[["issue_date", "contractors_business_name", "location_1", "permit_type", "valuation"]]

    # extract latitude and longitude from location_1
    # {'latitude': '33.99393', 'human_address': '{"address": "", "city": "", "state": "", "zip": ""}', 'needs_recoding': False, 'longitude': '-118.33429'}
    la_stripped["latitude"] = la_stripped["location_1"].apply(lambda x: x.split("'")[3] if type(x) == str else nan)
    la_stripped["longitude"] = la_stripped["location_1"].apply(lambda x: x.split("'")[13] if type(x) == str else nan)

    # drop rows without electrical contractors
    la_stripped = la_stripped[la_stripped.permit_type == "Electrical"]
    la_stripped.drop(columns=["location_1", "permit_type"], inplace=True)

    # drop rows with missing contractor_name
    la_stripped = la_stripped.dropna(subset=["contractors_business_name"])

    # split issue_date to only include date
    la_stripped["issue_date"] = la_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    write_s3(la_stripped, PROCESSED_BUCKET, "la.csv")
    print("Saved la.csv")
    return la_stripped

def strip_dataframes(city_list: str) -> list:
    """
    Strips the specified dataframes and saves them as csv files
    :param city_list: a list of the cities to strip
    """
    CITY_FUNCTIONS = {"austin": austin, "new_york": new_york, "chicago": chicago, "philly": philly, "mesa": mesa, "la": la}
    data_list: dict[str, pd.DataFrame] = {city: CITY_FUNCTIONS[city]() for city in city_list}
    return data_list

def combine_data(cities):
    """
    Combines .csv files in the directory "stripped_data" into a single file called "combinedData" store in the directory "combined_data"
    Each .csv file from the directory "stripped_data" will have an added column named "city" that is filled with the name of their respective city
    """
    # list constants that hold the identified similar names
    column_map = {
        "issued_date" : {"issue_date", "job_start_date", "issued_date", "permitissuedate"},
        "contractor" : {"contractor_company_name", "electrical_contractors","firm_name", "contractor_name", "contractorname", "contractors_business_name"},
        "latitude" : {"latitude","gis_latitude", "lat"},
        "longitude" : {"longitude","gis_longitude", "lng"},
        "valuation": {"valuation", "value", "market_value", "reported_cost", "electrical_valuation_remodel"}
    }

    # dataframe to store the combined data of all the cities after standardization
    combined_df = pd.DataFrame()

    # for loop that will append the data to the lists
    for city_name in cities.keys():
        # dataframe to store the standardized data for each city
        standardized_df = pd.DataFrame()
        city = cities[city_name]
        # go through column dictionary. dst_column is the standardized column name and src_column are the potential columns names from the data sets
        for dst_column, src_columns in column_map.items():
            # go through each src_column name and check if the src_column name is in the city"s columns.
            for src_column in src_columns:
                # if the src_column is found, than copy over the city"s src_column to the standardized_df dataframe"s dst_column
                if src_column in city.columns:
                    # copy over the city"s src_column to the standardized_df dataframe"s dst_column
                    standardized_df[dst_column] = pd.Series(city[src_column])
        # set the dataframe for the current city"s "city" column to the city_name which is the file"s name.
        standardized_df["city"] = city_name
        # concat the city data frame and the current combined data frame
        combined_df = pd.concat([combined_df, standardized_df])

    # set 0 valuation to nan
    combined_df["valuation"] = combined_df["valuation"].apply(lambda x: nan if x == 0 else x)

    # create csv with the dataframe with the combined data and store it in the combined_data folder
    write_s3(combined_df, COMBINED_BUCKET,"combinedData.csv")

if __name__ == "__main__":
    combine_data(strip_dataframes(["austin", "new_york", "chicago", "philly", "mesa", "la"]))
