import requests
import pandas as pd
import sqlite3
import yagmail
import utils
import yaml

# Load email config
with open("email_config.yml", "r") as f:
    email_config = yaml.safe_load(f)

# Setup Email details
yag = yagmail.SMTP(email_config["sender"], oauth2_file="oauth2_file.json")

# Open DB Connection
con = sqlite3.connect("contact_tracing_nsw.db")

# Prep database tables
utils.prep_database(con)

# GET NSW Data
res = requests.get("https://data.nsw.gov.au/data/dataset/0a52e6c1-bc0b-48af-8b45-d791a6d8e289/resource/f3a28eed-8c2a-437b-8ac1-2dab3cf760f9/download/venue-data.json")
res.encoding = 'utf-8-sig'

# Load Data into DataFrame & clean
df = pd.DataFrame(res.json()["data"]["monitor"])
df = utils.clean_dataframe(df)

# Load latest snapshot into tmp table
df.to_sql(name="contact_tracing_staging", con=con, schema="temp", if_exists="append", index=False)

# Break the staging table into INSERTs & UPDATEs and load into DataFrames
utils.load_staging_tables(con)
updated_records = pd.read_sql("select * from temp.contact_tracing_updates", con=con)
new_records = pd.read_sql("select * from temp.contact_tracing_inserts", con=con)

# If there are any new / updated rows, process and email to dist list
if len(new_records) > 0 or len(updated_records) > 0:

    # Email body
    contents = []

    # Create upto two sections depending on presences of new/updated records
    if len(new_records) > 0:
        contents.append("New Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(new_records))
    if len(updated_records) > 0:
        contents.append("Updated Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(updated_records))

    # Send email to dist list
    yag.send(bcc=email_config["dist_list"], subject="New NSW Contact Tracing Locations!", contents=contents)

    # Update Existing Records & Insert new records into database to mark them as processed
    utils.update_historical_records(con)
    new_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
    updated_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
else:
    # For logging purposes
    print("No updates!")

# Close DB connection
con.close()
