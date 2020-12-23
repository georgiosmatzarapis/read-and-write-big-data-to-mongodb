## How it works
1. Read the .csv file in chunks.
2. Convert the chunk to dataframe.
3. Split the dataframe in batches.
4. Insert each batch in MongoDB. 

## How to run
1. Insert your mongodb credentials.
2. Select the .csv file you want to insert.
3. To run the "insert_file_to_mongodb.py", the "batch_size" variable (max value 100k) must exactly divide the "rows_to_split_up" variable.