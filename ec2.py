import argparse
import configparser
import os
import socket
import time
from contextlib import contextmanager

import boto3
import paramiko


def pingserverport(host, port=22, timeout=3):
    """Attempt to open socket within timeout and report success"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        sock.close()
        return True
    except (socket.error, socket.timeout):
        return False


# pylint: disable=invalid-name,unused-argument,too-many-arguments
@contextmanager
def ec2instance(
    region_name,
    InstanceType,
    KeyName,
    ImageId,
    open_port,
    ping_timeout,
    IamInstanceProfile,
    security_group,
    subnet,
    **kwargs,
):
    """
    Spins an ec2 instance and terminates it on exit.
    Camel case arguments are passed to boto as is.

    :param str region_name: In which region to start the instance.
    :param str InstanceType: Type of instance as in ec2 api, e.g. i3.4xlarge.
    :param str KeyName: Key pair name. *Make sure* you have the pem file.
    :param str ImageId: AMI; must have docker installed.
    :param int open_port: Define a port that needs to be open for the instance
        to be considered ready.
    :param int ping_timeout: Define timeout on the ping operation.
    :param str IamInstanceProfile: Profile role to use.
    :param str security_group: Security group to use.
    """
    session = boto3.session.Session()
    client = session.client("ec2", region_name=region_name)

    response = client.run_instances(
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "VolumeSize": 30,
                    "VolumeType": "gp2",
                },
            },
        ],
        ImageId=ImageId,
        InstanceType=InstanceType,
        MaxCount=1,
        MinCount=1,
        KeyName=KeyName,
        Monitoring={"Enabled": False},
        InstanceInitiatedShutdownBehavior="terminate",
        IamInstanceProfile={"Name": IamInstanceProfile},
        NetworkInterfaces=[
            {
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": True,
                "Groups": [security_group],
                "SubnetId": subnet
            }
        ],
        UserData=kwargs.get("UserData")
    )

    iid = [instance["InstanceId"] for instance in response["Instances"]][0]

    try:
        open(f"ec2-instance-{iid}", "w").write("")
    except IOError:
        pass

    try:
        print("waiting to get ip")
        while True:
            a = client.describe_instances(InstanceIds=[iid])
            try:
                ip = [
                    instance["PrivateIpAddress"]
                    for reservation in a["Reservations"]
                    for instance in reservation["Instances"]
                ][0]
                break
            except KeyError:
                time.sleep(10)
        try:
            open(f"ec2-instance-{iid}", "w").write(ip)
        except IOError:
            pass
        print(f"got ip: {ip}")

        print("waiting for ssh port to be open")
        while not pingserverport(ip, open_port, ping_timeout):
            time.sleep(10)
        print("instance ssh port is open")

        yield ip
    finally:
        print("terminating instance")
        client.terminate_instances(InstanceIds=[iid])
        try:
            open(f"ec2-instance-{iid}", "w").write("terminated")
        except IOError:
            pass


def get_aws_access_keys():
    """Read aws credentials from environment or ~/.aws/credentials"""
    cfg = configparser.ConfigParser()
    cfg.read([os.path.expanduser("~/.aws/credentials")])
    try:
        aws_access_key_id = (
            os.environ.get("AWS_ACCESS_KEY_ID")
            or cfg["default"]["aws_access_key_id"]
        )

        aws_secret_access_key = (
            os.environ.get("AWS_SECRET_ACCESS_KEY")
            or cfg["default"]["aws_secret_access_key"]
        )

        assert aws_access_key_id and aws_secret_access_key

        return aws_access_key_id, aws_secret_access_key
    except Exception as _:
        raise IOError("couldn't find aws credentials")


def ssh_and_shutdown(ip, cmd, keyfilepath, username, **kwargs):
    """
    SSH into ip, run command, then shutdown

    :param str ip: Public ip of instance.
    :param str cmd: The command to run over ssh.
    :param str keyfilepath: File path for key file (.pem).
    :param str username: What username to use.
    """
    key = paramiko.RSAKey.from_private_key_file(keyfilepath)
    sshclient = paramiko.SSHClient()
    sshclient.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print("connecting with ssh")
    sshclient.connect(hostname=ip, username=username, pkey=key)
    try:
        print("executing command on instance")
        stdin, stdout, stderr = sshclient.exec_command(cmd)
        try:
            err = stderr.read()
            out = stdout.read()
            print(err.decode())
            print("\n\n\n")
            print(out.decode())
        finally:
            stdin.close()
    finally:
        print("scheduling immediate shutdown")
        sshclient.exec_command("sudo shutdown -h now")
        print("closing ssh connection")
        sshclient.close()
    