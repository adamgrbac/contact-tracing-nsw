import pandas as pd
from urllib.parse import unquote
import sqlite3


def prep_database(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    # Create history table if missing
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contact_tracing_hist (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_address varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp,
           row_start_tstp timestamp,
           row_end_tstp timestamp,
           row_status_code int
        );""")
        
    cur.execute("""DROP TABLE IF EXISTS temp.contact_tracing_staging""")
    # Create staging table
    cur.execute("""
        CREATE TABLE temp.contact_tracing_staging (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_address varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp
        );""")
    
    cur.execute("""DROP TABLE IF EXISTS temp.contact_tracing_inserts""")
    # Create history table if missing
    cur.execute("""
        CREATE TABLE temp.contact_tracing_inserts (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_address varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp,
           row_start_tstp timestamp,
           row_end_tstp timestamp,
           row_status_code int
        );""")
        
    cur.execute("""DROP TABLE IF EXISTS temp.contact_tracing_updates""")
    # Create history table if missing
    cur.execute("""
        CREATE TABLE temp.contact_tracing_updates (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_address varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp,
           row_start_tstp timestamp,
           row_end_tstp timestamp,
           row_status_code int
        );""")
    
    cur.close()
    

def htmlify(df: pd.DataFrame) -> str:
    """
    Description:
        htmlify takes in a Pandas DataFrame and returns a prettified
        html version for insertion into an email.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to transform
    Returns:
        output: str - html string
    """

    output = "<ul>"
    for row in df.to_dict(orient="records"):
        output += f"<li>({row['severity']}) {row['data_location']}, {row['data_suburb']} on {row['data_datetext']} between {row['data_timetext']}</li>"
    output += "</ul>"
    return output


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Description:
        clean_dataframe cleans a pandas DataFrame by correcting formatting
        issues and creating some desired columns.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to clean
        table_name: str - Name of contact tracing table (used as column value)
    Returns:
        df: pd.DataFrame - cleaned pandas Dataframe
    """

    col_names = list(df.columns)

    df["severity"] = df["HealthAdviceHTML"].apply(lambda x: "close" if "close" in x else "casual" if "casual" in x else "low")
    df["data_date"] = pd.to_datetime(df["Date"], format="%A %d %B %Y")
    df["data_location"] = df["Venue"].apply(unquote)
    df["data_address"] = df["Address"].apply(unquote)
    df["data_suburb"] = df["Suburb"].apply(unquote)
    df["data_datetext"] = df["Date"]
    df["data_timetext"] = df["Time"]
    df["data_added"] = pd.to_datetime(df["Last updated date"], format="%A %d %B %Y")

    df = df.drop(col_names, axis=1)
    
    df = df.groupby(["severity","data_date","data_location","data_address","data_suburb","data_datetext","data_timetext"]).agg({"data_added":max}).reset_index()

    return df
