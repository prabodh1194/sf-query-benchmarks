# sf-query-benchmarks
Adding some code to demonstrate the benchmarking for snowflake queries

This repo accompanies my [medium article](https://medium.com/p/833a1139ab01).

I have generated some test data which can be loaded in snowflake and then these benchmarking queries can be run against it.

### Obtain the data:
1. Download data from [google drive](https://drive.google.com/file/d/1Wr3j_ff930sdOk1zoHw8Ya6UjchgD7-s/view?usp=sharing)
    1. This data is a gzip parquet file.
    2. You can run the script `file_to_tbl.py` to load data into your snowflake account.
    3. Please setup your snowflake creds in the file `snowflake_connection.py`
2. You can generate your own data as well. I have provided the script `generate_data.py` which can be used to generate data. You can modify the script to generate data of your choice.
   1. Install the requirements `pip install -r requirements.txt` in repo root. Use `python@3.10`.
   2. Go to folder `data_generation`
   3. Create an empty folder `pq`
   4. Run `python generate_data.py`
   5. This will generate a parquet file `merge.parquet` in the same folder. Move it out one level up
   6. Run tge script `file_to_tbl.py` to load data into your snowflake account.


### Run the benchmarking queries:
1. Go to folder `benchmarking`
2. Run the script `pivot_query.py` to obtain the benchmarking results for pivot query.