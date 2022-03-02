"""this script checks that memory usage does not increase when data is deleted and ingested again"""
import subprocess as sp
from time import sleep
import requests
import sys
import os

if len(sys.argv) != 2:
    print("Usage:")
    print("python {} <public ip of data node>".format(sys.argv[0]))
    exit(1)

ip = sys.argv[1]


def datagen():
    """run_datagen runs datagen using the configured tremor.yaml file and blocks until it has completed and returns whether or not the command was successful"""
    try:
        sp.check_call(
            [
                "./datagen",
                "-s=custom",
                "--custom-config=./tremor.yaml",
                "--pilosa.index=tremor",
                "--pilosa.batch-size=300000",
                "--pilosa.hosts={}:10101".format(ip),
            ],
            stdout=sp.DEVNULL,
            stderr=sp.STDOUT,
        )
    except Exception as e:
        pass


def mem_use():
    """mem_use returns the memory usage of the data node and whether the request was successful"""
    response = requests.get("http://" + ip + ":10101/internal/mem-usage")
    if response.status_code != 200:
        return 0
    body = response.json()
    return body["totalUsed"]


def delete_all():
    """delete_all runs a Delete(All()) query on the data node and returns whether the delete was successful"""
    response = requests.post(
        "http://" + ip + ":10101/index/tremor/query", data="Delete(All())"
    )
    count = 0
    retry_period = 0.5
    while try_again(response) and (count < 9):
        print(response.text.strip())
        print(
            "There was a problem executing the Delete(All()) query. Trying again in {} seconds (attempt {}/9)".format(
                retry_period,
                count,
            )
        )
        count += 1
        sleep(retry_period)
        retry_period *= 2
        response = requests.post(
            "http://" + ip + ":10101/index/tremor/query", data="Delete(All())"
        )
    return response.status_code == 200


def try_again(response):
    if response.ok:
        # if we got a response but it was unsuccessful, try again
        return not response.json()["results"][0]
    # if we didn't get a good response, try again
    return True


def disk_use():
    response = requests.get("http://" + ip + ":10101/internal/disk-usage")
    if response.status_code != 200:
        return 0
    body = response.json()
    return body["usage"]


print("# memory usage tests")
usage = new_usage = 0
du = new_du = 0
datagen()
usage = mem_use()
du = disk_use()
print(
    "{},{},{},{}".format(
        "mem_usage", "disk_usage", "new_mem_greater", "new_disk_greater"
    )
)
print("{},{},{},{}".format(usage, du, None, None))
for i in range(5):
    variance = 0.25
    if delete_all():
        datagen()
        new_usage = mem_use()
        new_du = disk_use()
        print(
            "{},{},{},{}".format(
                usage,
                du,
                (new_usage / usage),
                (new_du / du),
            )
        )
        if (new_usage / usage) > (1 + variance):
            print(
                new_usage,
                "is greater than",
                usage,
                "by at least {}%.".format(variance * 100),
            )
            print(new_usage - usage, "is a lot of bytes.")
            print("Failing...")
            exit(1)
        if (new_du / du) > (1 + variance):
            print(
                new_du, "is greater than", du, "by at least {}%.".format(variance * 100)
            )
            print(new_du - du, "is a lot of bytes.")
            print("Failing...")
            exit(1)
    else:
        print("Couldn't execute Delete(All()) query in 9 tries. Failing...")
        exit(1)
print("Success")
exit(0)
