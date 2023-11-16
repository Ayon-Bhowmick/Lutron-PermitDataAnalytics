"""This script strips the raw data to only include the columns we need for the project."""

import pandas as pd
import logging
# from awsglue.context import GlueContext
import sys
from numpy import nan
import boto3
from io import StringIO

RAW_BUCKET = "lehigh-permit-raw-data-bucket"
STRIPED_BUCKET = "lehigh-permit-processed-data-bucket"

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
    austin_stripped = austin_stripped[["issue_date", "location", "contractor_company_name"]]

    # extract latitude and longitude from location column
    # {'latitude': '30.29406429', 'longitude': '-97.69323996', 'human_address': '{""address"": """", ""city"": """", ""state"": """", ""zip"": """"}'}
    austin_stripped["latitude"] = austin_stripped["location"].apply(lambda x: x.split(",")[0].split("'")[3] if type(x) == str else nan)
    austin_stripped["longitude"] = austin_stripped["location"].apply(lambda x: x.split(",")[1].split("'")[3] if type(x) == str else nan)

    # drop rows with missing contractor_company_name
    austin_stripped = austin_stripped.dropna(subset=["contractor_company_name"])
    austin_stripped.drop(columns=["location"], inplace=True)

    # split issue_date to only include date
    austin_stripped["issue_date"] = austin_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    write_s3(austin_stripped, STRIPED_BUCKET, "austin.csv")
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

    # drop rows with missing firm_name
    new_york_stripped = new_york_stripped.dropna(subset=["firm_name"])

    # split issue_date to only include date
    new_york_stripped["job_start_date"] = new_york_stripped["job_start_date"].apply(lambda x: x.split("T")[0] if type(x) == str else nan)
    write_s3(new_york_stripped, STRIPED_BUCKET, "new_york.csv")
    print("Saved new_york.csv")
    return new_york_stripped

def chicago() -> pd.DataFrame:
    """
    Extracts needed columns from the Chicago data and saves it as a csv file
    :return: chicago_stripped: the stripped data as a pandas dataframe
    """
    # read in raw data from s3
    chicago_raw = read_s3(RAW_BUCKET, "chicago.csv")

    # melt the dataframe to have one row per contractor
    chicago_stripped = pd.melt(chicago_raw, id_vars=["issue_date", "latitude", "longitude"], value_vars=[f"contact_{x}_name" for x in range(1, 12)].extend([f"contact_{x}_type" for x in range(1, 12)]), var_name="var", value_name="value")

    # drop rows with missing contractor_name
    chicago_stripped = chicago_stripped.dropna(subset=["value"])

    chicago_stripped["cid"] = chicago_stripped["var"].str.extract('(\d+)').astype(int)
    chicago_stripped = chicago_stripped[chicago_stripped["var"].str.contains("Contractor|Type")]
    chicago_stripped = chicago_stripped.pivot(index=["issue_date", "latitude", "longitude", "cid"], columns='var', values='value').reset_index()

    # # make dataframe of only electrical contractors
    # contractor_names = chicago_raw.filter(regex="contact_\d*_name")
    # contractor_trade = chicago_raw.filter(regex="contact_\d*_type")
    # contractor_trade.columns = contractor_names.columns
    # electrical_contractors = contractor_names[contractor_trade.eq("CONTRACTOR-ELECTRICAL")]

    # # convert electrical contractors to a series of tuples
    # chicago_raw["electrical_contractors"] = electrical_contractors.apply(lambda x: tuple(x.dropna()), axis=1)

    #  extract only the columns we need
    # chicago_stripped = chicago_raw[["issue_date", "electrical_contractors", "latitude", "longitude"]]

    # drop rows with missing electrical contractors
    # chicago_stripped = chicago_stripped[chicago_stripped.electrical_contractors.apply(lambda x: len(x) > 0)]

    # split issue_date to only include date
    chicago_stripped["issue_date"] = chicago_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    write_s3(chicago_stripped, STRIPED_BUCKET, "chicago.csv")
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
    write_s3(philly_stripped, STRIPED_BUCKET, "philly.csv")
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
    mesa_stripped = mesa_raw[["issued_date", "contractor_name", "latitude", "longitude"]]

    # drop rows with missing contractor_name
    mesa_stripped = mesa_stripped.dropna(subset=["contractor_name"])

    # split issue_date to only include date
    mesa_stripped["issued_date"] = mesa_stripped["issued_date"].apply(lambda x: x.split("T")[0])
    write_s3(mesa_stripped, STRIPED_BUCKET, "mesa.csv")
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
    la_stripped = la_raw[["issue_date", "contractors_business_name", "location_1", "permit_type"]]

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
    write_s3(la_stripped, STRIPED_BUCKET, "la.csv")
    print("Saved la.csv")
    return la_stripped

def strip_dataframes(city_list) -> list:
    """
    Strips the specified dataframes and saves them as csv files
    :param city_list: a list of the cities to strip
    """
    CITY_FUNCTIONS = {"austin": austin, "new_york": new_york, "chicago": chicago, "philly": philly, "mesa": mesa, "la": la}
    data_list = [CITY_FUNCTIONS[city]() for city in city_list]
    return data_list

if __name__ == "__main__":
    print(len(strip_dataframes(["philly"])))
