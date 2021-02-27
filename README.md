# arrow-csv-benchmark
EC2 block-size benchmark for arrow's read_csv

# Experiment details

This repo consists of a script that:

1. spins up an EC2 instance,
2. downloads the NY Yellow Taxi Trip dataset from January 2020,
3. reads it many times with pyarrow with different block sizes,
4. saves the results somewhere.

There's a short, basic analysis of the results in a notebook.

The data is not here yet, I'm still running the script a bit. I will upload the collected data later this weekend.

# How to run the script

The `run_benchmark.py` script takes a few required arguments in order to spin up EC2 instances. These include key-file pair, arn profile, etc.

The script aggressively makes sure that instances will shutdown after an alloted number of minutes, by:

1. Setting the startup script (UserData in EC2 parlance) to schedule a shutdown,
2. Using a context manager that uses boto to terminate instance when exiting,
3. Run the command shutdown when the single ssh command is done,
4. The ssh command being run, also itself, starts with a scheduled shutdown
5. The ssh command being run ends with a call to shutdown immediately.

The instances must shut down, since otherwise we pay!

Inevitably, of course, sometimes instances don't shut down, and I manually do it in the AWS console.
