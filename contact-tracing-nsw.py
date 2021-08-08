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
cur = con.cursor()

# GET NSW Data
res = requests.get("https://data.nsw.gov.au/data/dataset/0a52e6c1-bc0b-48af-8b45-d791a6d8e289/resource/f3a28eed-8c2a-437b-8ac1-2dab3cf760f9/download/venue-data.json")

# Load Data into DataFrame
df = pd.DataFrame(res.json()["data"]["monitor"])

# Clean DataFrame
df = utils.clean_dataframe(df)

# Load latest snapshot into tmp table
df.to_sql(name="contact_tracing_staging", con=con, schema="temp", if_exists="append", index=False)

# Create updates
cur.execute("""
    INSERT INTO temp.contact_tracing_updates
    SELECT
        staging.*,
        time('now') as row_start_tstp,
        time('3000-12-31 23:59:59') as row_end_tstp,
        1 as row_status_code
    FROM temp.contact_tracing_staging staging
    INNER JOIN contact_tracing_hist hist ON  staging.severity = hist.severity
                                        AND staging.data_date = hist.data_date
                                        AND staging.data_location = hist.data_location
                                        AND staging.data_address = hist.data_address
                                        AND staging.data_suburb = hist.data_suburb
                                        AND staging.data_datetext = hist.data_datetext
                                        AND staging.data_timetext = hist.data_timetext
                                        AND hist.row_status_code = 1
    WHERE 
        COALESCE(hist.data_added,'') <> COALESCE(staging.data_added,'')
    """)

# Create inserts
cur.execute("""
    INSERT INTO temp.contact_tracing_inserts
    SELECT
        staging.*,
        time('now') as row_start_tstp,
        time('3000-12-31 23:59:59') as row_end_tstp,
        1 as row_status_code
    FROM temp.contact_tracing_staging staging
    LEFT JOIN contact_tracing_hist hist ON  staging.severity = hist.severity
                                        AND staging.data_date = hist.data_date
                                        AND staging.data_location = hist.data_location
                                        AND staging.data_address = hist.data_address
                                        AND staging.data_suburb = hist.data_suburb
                                        AND staging.data_datetext = hist.data_datetext
                                        AND staging.data_timetext = hist.data_timetext
                                        AND hist.row_status_code = 1
    WHERE 
        hist.data_location IS NULL
    """)

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
    
    # Update historical records
    cur.execute("""
    UPDATE contact_tracing_hist
    SET row_status_code = 0, row_end_tstp = (SELECT row_start_tstp - 1 
                                             FROM temp.contact_tracing_updates 
                                             WHERE  contact_tracing_updates.severity = contact_tracing_hist.severity
                                                AND contact_tracing_updates.data_date = contact_tracing_hist.data_date
                                                AND contact_tracing_updates.data_location = contact_tracing_hist.data_location
                                                AND contact_tracing_updates.data_address = contact_tracing_hist.data_address
                                                AND contact_tracing_updates.data_suburb = contact_tracing_hist.data_suburb
                                                AND contact_tracing_updates.data_datetext = contact_tracing_hist.data_datetext
                                                AND contact_tracing_updates.data_timetext = contact_tracing_hist.data_timetext
                                                AND contact_tracing_hist.row_status_code = 1)
    WHERE EXISTS (SELECT data_location, data_address, data_datetext, data_timetext
                  FROM temp.contact_tracing_updates
                  WHERE  contact_tracing_updates.severity = contact_tracing_hist.severity
                    AND contact_tracing_updates.data_date = contact_tracing_hist.data_date
                    AND contact_tracing_updates.data_location = contact_tracing_hist.data_location
                    AND contact_tracing_updates.data_address = contact_tracing_hist.data_address
                    AND contact_tracing_updates.data_suburb = contact_tracing_hist.data_suburb
                    AND contact_tracing_updates.data_datetext = contact_tracing_hist.data_datetext
                    AND contact_tracing_updates.data_timetext = contact_tracing_hist.data_timetext
                    AND contact_tracing_hist.row_status_code = 1)""")

    # Insert new records into database to mark them as processed
    new_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
    updated_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
else:
    # For logging purposes
    print("No updates!")

# Close DB connection
con.close()
