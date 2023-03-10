import pandas as pd
import os

# austin
def austin():
    austin_raw = pd.read_csv("raw_data/austintexas.csv")
    austin_stripped = austin_raw[austin_raw.contractor_trade == "Electrical Contractor"]
    austin_stripped = austin_raw[["issue_date", "location", "contractor_company_name"]]
    latitude = []
    longitude = []
    for i in range(len(austin_stripped) - 1):
        # {'latitude': '30.29406429', 'longitude': '-97.69323996', 'human_address': '{""address"": """", ""city"": """", ""state"": """", ""zip"": """"}'}
        location_object = austin_stripped["location"][i]
        latitude_temp, longitude_temp = location_object.split(",")[:2]
        latitude.append(latitude_temp.split("'")[3])
        longitude.append(longitude_temp.split("'")[3])
    austin_stripped["latitude"] = pd.Series(latitude)
    austin_stripped["longitude"] = pd.Series(longitude)
    austin_stripped = austin_stripped.dropna(subset=["contractor_company_name"])
    austin_stripped.drop(columns=["location"], inplace=True)
    austin_stripped["issue_date"] = austin_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    austin_stripped.to_csv("stripped_data/austintexas.csv")
    print("Saved austintexas.csv")
    return austin_stripped

# new york
def new_york():
    new_york_raw = pd.read_csv("raw_data/cityofnewyork.csv")
    new_york_stripped = new_york_raw[["job_start_date","firm_name", 'gis_latitude', 'gis_longitude']]
    new_york_stripped = new_york_stripped.dropna(subset=["firm_name"])
    new_york_stripped["job_start_date"] = new_york_stripped["job_start_date"].apply(lambda x: x.split("T")[0])
    new_york_stripped.to_csv("stripped_data/cityofnewyork.csv")
    print("Saved cityofnewyork.csv")
    return new_york_stripped

# chicago
def chicago():
    chicago_raw = pd.read_csv("raw_data/cityofchicago.csv")
    chicago_stripped = chicago_raw[chicago_raw.contact_1_type == "CONTRACTOR-ELECTRICAL"]
    chicago_stripped = chicago_stripped[["issue_date", "contact_1_name", "latitude", "longitude"]]
    chicago_stripped = chicago_stripped.dropna(subset=["contact_1_name"])
    chicago_stripped["issue_date"] = chicago_stripped["issue_date"].apply(lambda x: x.split("T")[0])
    chicago_stripped.to_csv("stripped_data/cityofchicago.csv")
    print("Saved cityofchicago.csv")
    return chicago_stripped

# philly
def philly():
    philly_raw = pd.read_csv("raw_data/philly.csv")
    philly_stripped = philly_raw[philly_raw.permitdescription == "ELECTRICAL PERMIT"]
    # philly_stripped["location"] = f"({philly_stripped['lat']}, {philly_stripped['lng']})"
    # philly_stripped = philly_stripped[["permitnumber", "permittype", "permitissuedate", "location", "contractorname"]]
    philly_stripped = philly_stripped[["permitissuedate", "contractorname", "lat", "lng"]]
    philly_stripped = philly_stripped.dropna(subset=["contractorname"])
    philly_stripped["permitissuedate"] = philly_stripped["permitissuedate"].apply(lambda x: x.split(" ")[0])
    philly_stripped.to_csv("stripped_data/philly.csv")
    print("Saved philly.csv")
    return philly_stripped

# mesa
# can't filter by contractor trade
def mesa():
    mesa_raw = pd.read_csv("raw_data/mesaaz.csv")
    mesa_stripped = mesa_raw[["issued_date", "contractor_name", "latitude", "longitude"]]
    mesa_stripped = mesa_stripped.dropna(subset=["contractor_name"])
    mesa_stripped["issued_date"] = mesa_stripped["issued_date"].apply(lambda x: x.split("T")[0])
    mesa_stripped.to_csv("stripped_data/mesaaz.csv")
    print("Saved mesaaz.csv")
    return mesa_stripped

# la
def la():
    la_raw = pd.read_csv("raw_data/lacity.csv")
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
    la_stripped.to_csv("stripped_data/lacity.csv")
    print("Saved lacity.csv")
    return la_stripped

city_functions = [austin, new_york, chicago, philly, mesa, la]
data_list = [city() for city in city_functions]
