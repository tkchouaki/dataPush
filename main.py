from paramiko import SSHClient
from scp import SCPClient
import tempfile
import os
import yaml
import sys

REQUIRED_CONFIG_ELEMENTS = ["ssh_key", "ssh_host", "ssh_user", "source", "destination"]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Exactly one command line argument is expected")

    with open(sys.argv[1], "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    for element in REQUIRED_CONFIG_ELEMENTS:
        if element not in config:
            raise Exception("Required element %s not found in config" % element)
        if not isinstance(config[element], str):
            raise Exception("Required element %s must be a string" % element)

    if not os.path.isfile(config["ssh_key"]):
        raise Exception("SSH key file %s not found" % config["ssh_key"])

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(config["ssh_host"], username=config["ssh_user"], key_filename=config["ssh_key"])
    ssh.exec_command("mkdir -p " + config["destination"])

    scp = SCPClient(ssh.get_transport())

    _, out, _ = ssh.exec_command("ls -a" + config["destination"])

    destination_content = out.readlines()

    temp_dir = tempfile.TemporaryDirectory(prefix="temp_sync", dir="./")

    already_uploaded_server = set()

    if ".already_uploaded.txt" in destination_content:
        scp.get(config["destination"] + ".already_uploaded.txt", temp_dir.name + "/already_uploaded_server.txt")
        with open(temp_dir.name + "/already_uploaded_server.txt") as f:
            already_uploaded_server = set(f.read().splitlines())

    already_uploaded_client = set()
    if os.path.isfile("already_uploaded.txt"):
        with open("already_uploaded.txt") as f:
            already_uploaded_client = set(f.read().splitlines())

    content = []
    for dir_path, _, files in os.walk(config["source"]):
        if len(files) > 0:
            content.extend([(dir_path[len(config["source"])+1::] + "/" + file) for file in files])

    for c in content:
        if c not in already_uploaded_client and c not in already_uploaded_server:
            ssh.exec_command('mkdir -p "' + config["destination"] + '/' + os.path.dirname(c)+'"')
            print("Uploading " + c)
            scp.put(config["source"] +"/" + c, config["destination"] + "/" + c)

    scp.close()



"""
source = ""
destination = ""

ssh = SSHClient()
ssh.load_system_host_keys()
ssh.connect('', username="abdo", key_filename="")
ssh.exec_command("mkdir -p " + destination)

# SCPCLient takes a paramiko transport as an argument
scp = SCPClient(ssh.get_transport())

_, out, _ = ssh.exec_command("ls -a" + destination)

destination_content = out.readlines()

temp_dir = tempfile.TemporaryDirectory(prefix="temp_sync", dir="./")

already_uploaded_server = set()

if ".already_uploaded.txt" in destination_content:
    scp.get(destination + ".already_uploaded.txt", temp_dir.name + "/already_uploaded_server.txt")
    with open(temp_dir.name + "/already_uploaded_server.txt") as f:
        already_uploaded_server = set(f.read().splitlines())

already_uploaded_client = set()
if os.path.isfile("already_uploaded.txt"):
    with open("already_uploaded.txt") as f:
        already_uploaded_client = set(f.read().splitlines())

content = []
for dir_path, _, files in os.walk(source):
    if len(files) > 0:
        content.extend([(dir_path[len(source)+1::] + "/" + file) for file in files])

for c in content:
    if c not in already_uploaded_client and c not in already_uploaded_server:
        ssh.exec_command('mkdir -p "' + destination + '/' + os.path.dirname(c)+'"')
        print("Uploading " + c)
        scp.put(source +"/" + c, destination + "/" + c)


scp.close()
"""