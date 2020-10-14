# Parameters for script.
# These values reflect an actual work load I had that was slow.
num_gigabytes = 10  # Approximate number of giga bytes of the csv
str_columns, str_lens = 200, 13  # How many string columns to include, and what is the uniform length of all strings
float_columns = 200  # How many float columns to include
null_columns = 20  # How many all-null columns to include
num_rounds = 5  # How many calls to read_csv to make
backend = "loky"  # Joblib backend for parallelising creation of csv
n_jobs = -1  # Number of jobs for csv creation parallelisation

#####################################################################

from io import BytesIO
import pyarrow.csv
import time
import pyarrow as pa
import os
import pandas as pd
import numpy as np
import joblib
from multiprocessing import cpu_count

@joblib.delayed
def create_csv(
    num_rows,
    str_columns, str_lens,
    float_columns,
    null_columns,
    random_seed, columns_permutation,
    header
    ):
    """Creates random csv with given parameters"""
    # Create random string columns
    rand = np.random.RandomState(random_seed)
    A, Z = np.array(["A","Z"]).view("int32")
    strs = rand.randint(
        low=A,high=Z,size=(num_rows, str_columns, str_lens),dtype="int32"
    ).view(f"U{str_lens}")[..., 0]

    # Create random float columns
    floats = rand.randn(num_rows, float_columns)

    # Create null columns
    nulls = [[pd.NA] * null_columns] * num_rows

    # Put columns into a pandas DataFrame in same random order
    df = pd.concat([
        pd.DataFrame(
            values,
            columns=[f"{name}_{i}" for i in range(len(values[0]))]
        )
        for name, values in [
            ("strs", strs), ("floats", floats), ("nulls", nulls)
        ]
    ], axis=1)
    df = df.iloc[:, columns_permutation]

    # Create the csv for the DataFrame
    data_raw = df.to_csv(sep="|", header=header).encode()
    return data_raw

if __name__ == "__main__":
    print("pid:", os.getpid())  # Print pid so I can run py-spy on it

    # Compute a random perutation for the columns order.
    columns_permutation = np.random.RandomState(2).permutation(
        str_columns + float_columns + null_columns
    )
    
    # Approximate number of rows needed to fulfill approximate number of gigabytes
    row_gigabyte = (
        (
            str_columns * (str_lens + 1)
            + float_columns * (19 + 1)  # The floats have aound 19 characters on average
            + null_columns + 2
        ) * cpu_count() / 1024**3
    )
    num_rows = int(num_gigabytes / row_gigabyte)
    
    print("1/3 Creating csv chunks")

    # Parallelise creation of csv
    with joblib.Parallel(n_jobs=n_jobs, backend=backend) as pool:
        data_raw = pool(
            create_csv(
                num_rows, str_columns, str_lens,
                float_columns, null_columns, i, columns_permutation, i == 0
            )
            for i in range(cpu_count())
        )

    print("2/3 Creating bytes")
    
    total_size = sum(map(len, data_raw)) / 1024**3
    data_bytes = BytesIO()
    for b in data_raw:
        data_bytes.write(b)

    times = []
    parse_options = pyarrow.csv.ParseOptions(delimiter="|")

    for i in range(num_rounds):
        data_bytes.seek(0)  # Go back to beginning of buffer
        print(f"starting {i}/{num_rounds}")
        
        # Actual call to read_csv
        t1 = time.time()
        a = pyarrow.csv.read_csv(data_bytes, parse_options=parse_options)
        t2 = time.time()
        
        times.append(t2 - t1)
        print(f"done in {times[-1]:.2f} secs, {total_size / times[-1]:.2f} GiB/s")
        del a
        
    print(
        "processed", total_size,
        "in max speed of", total_size / min(times), "GiB/s"
    )
    # Print all times
    print(", ".join("%.2f" % v for v in (total_size / np.array(times))))

