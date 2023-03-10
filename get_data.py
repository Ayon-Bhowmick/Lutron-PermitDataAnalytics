import pandas as pd

#import ssl
#ssl._create_default_https_context = ssl._create_unverified_context

# Define the URL of the database
philly_data = 'https://phl.carto.com/api/v2/sql?filename=permits&format=csv&skipfields=cartodb_id,the_geom,the_geom_webmercator&q=SELECT%20*,%20ST_Y(the_geom)%20AS%20lat,%20ST_X(the_geom)%20AS%20lng%20FROM%20permits%20WHERE%20permitissuedate%20%3E=%20%272016-01-01%27'

# Use Pandas to read the CSV file from the URL
df = pd.read_csv(philly_data)

# Save the CSV file to the current directory
df.to_csv('raw_data/philly.csv', index=False)
