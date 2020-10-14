num_gigabytes = 10
str_columns, str_lens = 200, 13
float_columns = 200
null_columns = 20
num_rounds = 5
backend = "loky"
n_jobs = -1

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
    rand = np.random.RandomState(random_seed)
    A, Z = np.array(["A","Z"]).view("int32")
    strs = rand.randint(
        low=A,high=Z,size=(num_rows, str_columns, str_lens),dtype="int32"
    ).view(f"U{str_lens}")[..., 0]

    floats = rand.randn(num_rows, float_columns)

    nulls = [[pd.NA] * null_columns] * num_rows

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

    data_raw = df.to_csv(sep="|", header=header).encode()
    return data_raw

if __name__ == "__main__":
    print("pid:", os.getpid())

    columns_permutation = np.random.RandomState(2).permutation(
        str_columns + float_columns + null_columns
    )
    row_gigabyte = (
        (
            str_columns * (str_lens + 1)
            + float_columns * (19 + 1)
            + null_columns + 2
        ) * cpu_count() / 1024**3
    )
    num_rows = int(num_gigabytes / row_gigabyte)
    print("1/3 Creating csv chunks")

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
        data_bytes.seek(0)
        print(f"starting {i}/{num_rounds}")
        t1 = time.time()
        a = pyarrow.csv.read_csv(data_bytes, parse_options=parse_options)
        t2 = time.time()
        times.append(t2 - t1)
        print(f"done in {times[-1]:.2f} secs, {total_size / times[-1]:.2f} GiB/s")
        del a
        
    print(
        "processed", total_size,
        "in", total_size / np.median(times), "GiB/s"
    )
    print(", ".join("%.2f" % v for v in (total_size / np.array(times))))

