#!/bin/python
import argparse
import threading
import time

from ec2 import ec2instance, get_aws_access_keys, ssh_and_shutdown


def create_ssh_command(
    maximum_mins, aws_access_key_id, aws_secret_access_key, docker_image,
    inputpath, outpath, InstanceType, rounds_per_output, max_output_files,
    csv_copies
):
    pip_install = "pip install pyarrow==3.0.0 requests boto3 smart_open"
    
    python_1_statements = [
        f"import requests",
        f"fpath = '/tmp/a.csv'",
        f"x = requests.get('{inputpath}').content",
        f"f = open(fpath, 'wb')",
        f"f.write(x)",
        f"[f.write(x[x.find(b'\\r\\n'):]) for _ in range({csv_copies - 1})]"
    ]
    
    python_1 = (
        f"python -c \\\"{'; '.join(python_1_statements)}\\\" "
    )
    
    big_statement = (
        f"[smart_open.open(f'{outpath}{InstanceType}.{{uuid.uuid1()}}', 'w')"
        f".write(f'{{[[block_size, t1, t2, {csv_copies}]"
        f" for _ in range({rounds_per_output})"
        f" for block_size in [64*1024] + [2**i * 1024**2 for i in range(9)]"
        f" for t1, _, t2 in"
        f" [(time.time(),pyarrow.csv.read_csv(fpath,"
        f" read_options=pyarrow.csv.ReadOptions(block_size=block_size)),"
        f" time.time())]]}}') for _ in range({max_output_files})]"
    )
    
    python_2_statements = [
        "fpath = '/tmp/a.csv'",
        "import smart_open",
        "import uuid",
        "import time",
        "import pyarrow",
        "import pyarrow.csv",
        big_statement
    ]
    
    python_2 = (
        f"python -c \\\"{'; '.join(python_2_statements)}\\\""
    )
    
    docker_bash_command = f"{pip_install} && {python_1} && {python_2}"
    
    docker_command = (
        f"docker run --rm"
        f" --env AWS_ACCESS_KEY_ID={aws_access_key_id}"
        f" --env AWS_SECRET_ACCESS_KEY={aws_secret_access_key}"
        f" {docker_image} /bin/bash -c \"{docker_bash_command}\""
    )
    
    return (
        f"sudo shutdown -h {maximum_mins}"
        f"&& {docker_command}"
        f"  && sudo shutdown -h now"
    )


def run_one(
    keyfilepath, aws_access_key_id, aws_secret_access_key, outpath,
    maximum_mins, InstanceType, csv_copies, inputpath, rounds_per_output,
    max_output_files, docker_image, **kwargs
):
    if aws_access_key_id is None or aws_secret_access_key is None:
        aws_access_key_id, aws_secret_access_key = get_aws_access_keys()

    UserData = (
        "#!/bin/bash"
        f"sudo shutdown -h {maximum_mins}"
    )
    
    ssh_command = create_ssh_command(
        maximum_mins, aws_access_key_id, aws_secret_access_key, docker_image,
        inputpath, outpath, InstanceType, rounds_per_output, max_output_files,
        csv_copies
    )

    with ec2instance(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        InstanceType=InstanceType,
        UserData=UserData,
        **kwargs
    ) as ip:
        try:
            ssh_and_shutdown(
                ip,
                ssh_command,
                keyfilepath,
                "ubuntu",
            )
        except:
            pass

def argsparser():
    parser = argparse.ArgumentParser(
        description="Spin EC2 instances that benchmark pyarrow.csv.read_csv."
    )
    parser.add_argument("--KeyName", type=str, help="KeyName passed to boto")
    parser.add_argument(
        "--keyfilepath", type=str, help="local path to key pair"
    )
    parser.add_argument("--security_group", type=str)
    parser.add_argument("--subnet", type=str)
    parser.add_argument("--IamInstanceProfile", type=str)
    parser.add_argument(
        "--outpath", type=str, help="s3 prefix of where to save results"
    )

    # Arguments with defaults
    parser.add_argument("--docker_image", type=str, default="python:3.8")
    parser.add_argument(
        "--maximum_mins",
        type=int,
        default=120,
        help="maximum minutes an instance can be up before terminating"
    )
    parser.add_argument(
        "--inputpath",
        type=str,
        default=(
            "https://s3.amazonaws.com/nyc-tlc/trip+data/"
            "yellow_tripdata_2020-01.csv"
        ),
        help=(
            "path to csv to use,"
            " default is NY yellow taxi trip records from January 2020"
        )
    )
    parser.add_argument(
        "--rounds_per_output",
        type=int,
        default=3,
        help="number of rounds per block size per output file"
    )
    parser.add_argument(
        "--max_output_files",
        type=int,
        default=100,
        help="maximum number of output files"
    )
    parser.add_argument(
        "--csv_copies",
        type=int,
        default=4,
        help="how many times to concatenate csv to itself"
    )
    parser.add_argument("--aws_access_key_id", type=str, default=None)
    parser.add_argument("--aws_secret_access_key", type=str, default=None)
    parser.add_argument("--region_name", type=str, default="us-east-2")
    parser.add_argument(
        "--instances_at_a_time",
        type=int,
        default=5,
        help="how many ec2 instances to spin at the same time"
    )
    parser.add_argument(
        "--ImageId",
        type=str,
        default="ami-09f77b37a0d32243a",
        help="Must have docker installed.",
    )
    parser.add_argument(
        "--username", type=str, default="ubuntu", help="user for ssh"
    )
    parser.add_argument(
        "--open_port",
        type=int,
        default=22,
        help="port that will be open for ssh"
    )
    parser.add_argument("--ping_timeout", type=int, default=3)

    return parser


ec2_instance_types = [
    "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge", "m5.8xlarge",
    "m5.12xlarge", "m5.16xlarge", "c5.2xlarge", "c5.4xlarge", "c5.9xlarge",
    "c5.12xlarge", "c5.18xlarge", "r5a.large", "r5a.xlarge", "r5a.2xlarge",
    "r5a.4xlarge", "r5a.8xlarge", "r5a.12xlarge", "r5a.16xlarge",
    "r5a.24xlarge"
]


def run_many(maximum_mins, instances_at_a_time, **kwargs):
    threads = []

    # EC2 doesn't like it when I run start too many instances at the same time.
    for pos in range(0, len(ec2_instance_types), instances_at_a_time):
        for InstanceType in ec2_instance_types[pos:][:instances_at_a_time]:
            threads.append(threading.Thread(
                target=run_one,
                kwargs={
                    "maximum_mins": maximum_mins,
                    "InstanceType": InstanceType,
                    **kwargs
                }
            ))
            threads[-1].start()
        time.sleep(maximum_mins * 60)


def main():
    """Run EC2 pyarrow.csv.read_csv benchmark with arguments passed to script"""
    args = vars(argsparser().parse_args())
    run_many(**args)


if __name__ == '__main__':
    main()
