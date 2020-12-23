import pandas as pd
import time, os, sys
from pymongo import MongoClient


def connect_to_mongodb_collection():
    """Credentials for mongodb connection."""

    uri = #NOTE: INSERT YOUR URI
    client = MongoClient(uri)
    collection = client.#NOTE: CHOOSE YOUR COLLECTION
    return collection


def file_length(file_name):
    """Retrieve length of a file."""

    with open(file_name) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def dataset_path(file_name):
    """Specify dataset path."""

    file_directory = os.path.dirname(os.path.abspath(__file__))
    parent_directory = os.path.dirname(file_directory)
    dataset_path = os.path.join(parent_directory, f'yahoo_dataset_music/{file_name}')
    return dataset_path


def mongodb_inserts(db_connection, dataframe, file_length, batch_size):
    """Split up each dataframe in batches and insert each batch in mongodb."""

    batch_counter = 0
    batch_documents = [i for i in range(batch_size)]
    store_to_mongo_collection = db_connection

    for index, row in dataframe.iterrows():
        try:
            document = {"user_id": row['UserId'], "rate": row['Rate']}
            batch_documents[index % batch_size] = document
            if (index + 1) % batch_size == 0:
                store_to_mongo_collection.insert_many(batch_documents)
                
            elif (index + 1) == file_length:
                del batch_documents[(index + 1) % batch_size:-1]
                batch_documents.pop(-1)
                store_to_mongo_collection.insert_many(batch_documents)

        except:
            print(f'Unexpected error: {sys.exc_info()[0]}, for index  {index}')
            raise


def file_to_df_and_store_to_mongo(file_name, chunk_size, file_length, batch_size):
    """Split file in dataframe chunks and insert each in mongodb."""

    df_counter = 0
    time_counter = 0
    file = dataset_path(file_name)
    for df_chunk in pd.read_csv(file, chunksize=chunk_size, sep="\t", names=["UserId", "Rate"], encoding="utf-8"):
        df_counter = df_counter + 1
        print(f'\nDataframe-{df_counter} with length {len(df_chunk.index)} is ready !\n')
        # insert dataframe to monogodb ->
        start_time = time.time()
        mongodb_inserts(connect_to_mongodb_collection(), df_chunk, file_length, batch_size)
        round_time = time.time() - start_time
        print(f"\n--- {round_time} seconds to store 'Dataframe-{df_counter}'. ---\n")
        time_counter += round_time

    print('\n[LOGS]')
    print(f'[INFO] Total rows of selected file -> {file_length}.')
    print(f'[INFO] Total number of created dataframes -> {df_counter}.')
    print(f'[INFO] Total documents which inserted in mongodb -> {connect_to_mongodb_collection().count_documents({})}.')
    print(f'[INFO] Total minutes to insert in mongodb -> {time_counter / 60}.\n\n')



if __name__ == "__main__":

    print('\nPlease insert the following fields: ')
    file = input("- file which you want to process with: ")
    rows_to_split_up = input("- number of rows which you want to split up your file for process: ")
    batch_size = input("- batch size: ")
    file_input_length = file_length(dataset_path(file))
    file_to_df_and_store_to_mongo(file, int(rows_to_split_up), file_input_length, int(batch_size))