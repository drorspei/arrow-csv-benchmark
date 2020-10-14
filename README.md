# arrow-csv-benchmark
Short benchmark for arrow's read_csv

# Why

I made this repo after experiencing low read speeds (0.5GiB/s) on real work csvs.

# What this does

1. Generates a big csv with many string, float, and null columns, using joblib for parallelization,
2. Puts the csv into a `BytesIO` object,
3. Calls `pyarrow.csv.read_csv` a few times on the csv bytes.

The Dockerfile sets up a minimal container for running the benchmark.

# My Results

Running this on Azure, machine size `Standard E48s_v3 (48 vcpus, 384 GiB memory)`, unused other than this benchmark, consistently shows speeds of less than 1GiB/s, and often below 0.5GiB/s.

Included in the repo are profiling dumps, made manually with py-spy. I started them 5 seconds after the beginning of each `read_csv`, and stopped them after about 15 seconds. This was always more than 5 seconds before the `read_csv` finished.

If the profiles are to be trusted, there is considerable time spent in the shared pointer's lock mechanisms. As for the reading of the bytes, I'm not sure what goes into this or why it takes time.
