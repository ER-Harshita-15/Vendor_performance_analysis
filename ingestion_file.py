import pandas as pd
import os 
from sqlalchemy import create_engine
import logging
import time


# Setting up logging
logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a')

engine = create_engine('sqlite:///inventory.db')

#Scripting the data into a SQLite database
def ingest_db(df,table_name, engine):
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Function to read CSV files and return a DataFrame
def load_raw_data():
    start=time.time()
    logging.info("-----------------Ingestion Started-----------------")
    for file in os.listdir('data'):
        if '.csv' in file:
             df= pd.read_csv(os.path.join('data', file))
             logging.info("Ingesting file: %s", file)
             ingest_db(df, file[:-4], engine)


    end=time.time()
    total_time=(end-start)/60
    logging.info("-----------------Ingestion Completed-----------------")
    logging.info("All files ingested successfully.")
    logging.info("Total time taken for ingestion: %.2f minutes", total_time)

if __name__ == "__main__":
    load_raw_data()
       




    

    