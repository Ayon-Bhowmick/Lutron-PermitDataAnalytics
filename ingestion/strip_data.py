"""This script strips the raw data to only include the columns we need for the project."""

import pandas as pd
import logging
import sys

def austin() -> pd.DataFrame:
    """
    Extracts needed columns from the Austin data and saves it as a csv file
    :return austin_stripped: the stripped data as a pandas dataframe
    """
    AUSTIN_RAW_PATH = "./raw_data/austin.csv"
    AUSTIN_STRIPED_PATH = "./stripped_data/austin.csv"
    austin_raw = pd.read_csv(AUSTIN_RAW_PATH)
    austin_stripped = austin_raw[austin_raw.contractor_trade == "Electrical Contractor"]
    austin_stripped = austin_stripped[["issue_date", "location", "contractor_company_name"]]
    latitude = []
    longitude = []
    for i in range(len(austin_stripped) - 1):
        # {'latitude': '30.29406429', 'longitude': '-97.69323996', 'human_address': '{""address"": """", ""city"": """", ""state"": """", ""zip"": """"}'}
        location_object = austin_stripped["location"][i]
        if type(location_object) != str:
            latitude.append("nan")
            longitude.append("nan")
            continue
        latitude_temp, longitude_temp = location_object.split(",")[:2]
        latitude.append(latitude_temp.split("'")[3])
        longitude.append(longitude_temp.split("'")[3])
    austin_stripped["latitude"] = pd.Series(latitude)
    austin_stripped["longitude"] = pd.Series(longitude)
    austin_stripped = austin_stripped.dropna(subset=["contractor_company_name"])
    austin_stripped.drop(columns=["location"], inplace=True)
    austin_stripped["issue_date"] = austin_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    austin_stripped.to_csv(AUSTIN_STRIPED_PATH)
    logging.info("Saved austin.csv")
    return austin_stripped

def new_york() -> pd.DataFrame:
    """
    Extracts needed columns from the New York data and saves it as a csv file
    :return new_york_stripped: the stripped data as a pandas dataframe
    """
    NEW_YORK_RAW_PATH = "./raw_data/new_york.csv"
    NEW_YORK_STRIPPED_PATH = "./stripped_data/new_york.csv"
    new_york_raw = pd.read_csv(NEW_YORK_RAW_PATH)
    new_york_stripped = new_york_raw[["job_start_date","firm_name", 'gis_latitude', 'gis_longitude']]
    new_york_stripped = new_york_stripped.dropna(subset=["firm_name"])
    if type(new_york_stripped["job_start_date"]) != str:
        new_york_stripped["job_start_date"] = "nan"
    else:
        new_york_stripped["job_start_date"] = new_york_stripped["job_start_date"].apply(lambda x: x.split("T")[0])
    new_york_stripped.to_csv(NEW_YORK_STRIPPED_PATH)
    logging.info("Saved new_york.csv")
    return new_york_stripped

def chicago() -> pd.DataFrame:
    """
    Extracts needed columns from the Chicago data and saves it as a csv file
    :return: chicago_stripped: the stripped data as a pandas dataframe
    """
    CHICAGO_RAW_PATH = "./raw_data/chicago.csv"
    CHICAGO_STRIPPED_PATH = "./stripped_data/chicago.csv"
    chicago_raw = pd.read_csv(CHICAGO_RAW_PATH)
    chicago_stripped = chicago_raw[chicago_raw.contact_1_type == "CONTRACTOR-ELECTRICAL"]
    chicago_stripped = chicago_stripped[["issue_date", "contact_1_name", "latitude", "longitude"]]
    chicago_stripped = chicago_stripped.dropna(subset=["contact_1_name"])
    chicago_stripped["issue_date"] = chicago_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    chicago_stripped.to_csv(CHICAGO_STRIPPED_PATH)
    logging.info("Saved chicago.csv")
    return chicago_stripped

def philly()-> pd.DataFrame:
    """
    Extracts needed columns from the Philadelphia data and saves it as a csv file
    :return: philly_stripped: the stripped data as a pandas dataframe
    """
    PHILLY_RAW_PATH = "./raw_data/philly.csv"
    PHILLY_STRIPPED_PATH = "./stripped_data/philly.csv"
    philly_raw = pd.read_csv(PHILLY_RAW_PATH)
    philly_stripped = philly_raw[philly_raw.permitdescription == "ELECTRICAL PERMIT"]
    philly_stripped = philly_stripped[["permitissuedate", "contractorname", "lat", "lng"]]
    philly_stripped = philly_stripped.dropna(subset=["contractorname"])
    philly_stripped["permitissuedate"] = philly_stripped["permitissuedate"].apply(lambda x: x.split(" ")[0])

    # concat the market_value column to the philly_stripped dataframe from the second philly csv using lat lng as a shared key
    philly_raw2 = pd.read_csv("./raw_data/philly_valuation.csv")
    philly_raw2 = philly_raw2[["lat", "lng", "market_value"]]
    philly_stripped = pd.merge(philly_stripped, philly_raw2, on=["lat", "lng"], how="left")
    philly_stripped.to_csv(PHILLY_STRIPPED_PATH)
    logging.info("Saved philly.csv")
    return philly_stripped

def mesa()-> pd.DataFrame:
    """
    Extracts needed columns from the Mesa data and saves it as a csv file
    Can't filter by contractor trade
    :return: mesa_stripped: the stripped data as a pandas dataframe
    """
    MESA_RAW_PATH = "./raw_data/mesa.csv"
    MESA_STRIPPED_PATH = "./stripped_data/mesa.csv"
    mesa_raw = pd.read_csv(MESA_RAW_PATH)
    mesa_stripped = mesa_raw[["issued_date", "contractor_name", "latitude", "longitude"]]
    mesa_stripped = mesa_stripped.dropna(subset=["contractor_name"])
    mesa_stripped["issued_date"] = mesa_stripped["issued_date"].apply(lambda x: x.split("T")[0])
    mesa_stripped.to_csv(MESA_STRIPPED_PATH)
    logging.info("Saved mesa.csv")
    return mesa_stripped

def la()-> pd.DataFrame:
    """
    Extracts needed columns from the Los Angeles data and saves it as a csv file
    :return: la_stripped: the stripped data as a pandas dataframe
    """
    LA_RAW_PATH = "./raw_data/la.csv"
    LA_STRIPPED_PATH = "./stripped_data/la.csv"
    la_raw = pd.read_csv(LA_RAW_PATH)
    la_stripped = la_raw[["issue_date", "contractors_business_name", "location_1", "permit_type"]]
    latitude = []
    longitude = []
    for i in range(len(la_stripped) - 1):
        # {'latitude': '33.99393', 'human_address': '{"address": "", "city": "", "state": "", "zip": ""}', 'needs_recoding': False, 'longitude': '-118.33429'}
        location_object = la_stripped["location_1"][i]
        if type(location_object) != str:
            latitude.append("nan")
            longitude.append("nan")
            continue
        location_object = location_object.split("'")
        latitude.append(location_object[3])
        longitude.append(location_object[13])
    la_stripped["latitude"] = pd.Series(latitude)
    la_stripped["longitude"] = pd.Series(longitude)
    la_stripped = la_stripped[la_stripped.permit_type == "Electrical"]
    la_stripped.drop(columns=["location_1", "permit_type"], inplace=True)
    la_stripped = la_stripped.dropna(subset=["contractors_business_name"])
    la_stripped["issue_date"] = la_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    la_stripped.to_csv(LA_STRIPPED_PATH)
    logging.info("Saved la.csv")
    return la_stripped

def strip_dataframes(city_list) -> list:
    """
    Strips the specified dataframes and saves them as csv files
    :param city_list: a list of the cities to strip
    """
    city_functions = [globals()[x] for x in city_list if x in globals() and callable(globals()[x])]
    data_list = [city() for city in city_functions]
    return data_list

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.info(len(strip_dataframes(["austin", "new_york", "chicago", "philly", "mesa", "la"])))
