from paramiko import SSHClient
from paramiko.client import AutoAddPolicy
from scp import SCPClient
import tempfile
import os
import yaml
import sys
import time
from tqdm_loggable.auto import tqdm
import logging

REQUIRED_CONFIG_ELEMENTS = ["ssh_key", "ssh_host", "ssh_user", "source", "destination", "db_path"]

class DataPush(object):
    logging.basicConfig(level=logging.INFO)
    REQUIRED_CONFIG_ELEMENTS = ["ssh_key", "ssh_host", "ssh_user", "source", "destination"]
    DEFAULTS = {"update_frequency": 60, "ignore_extensions": []}

    def __init__(self, config_path):
        if not os.path.isfile(config_path):
            raise Exception("Config file %s does not exist!" % config_path)
        with open(config_path, "r") as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)
        for element in DataPush.REQUIRED_CONFIG_ELEMENTS:
            if element not in self.config:
                raise Exception("Required element %s not found in config" % element)
            if not isinstance(self.config[element], str) and not isinstance(self.config[element], int):
                raise Exception("Required element %s must be a string or an integer" % element)
        if not os.path.isfile(self.config["ssh_key"]):
            raise Exception("SSH key file %s not found" % self.config["ssh_key"])
        if not os.path.isdir(self.config["source"]):
            raise Exception("Source directory %s does not exit" % self.config["source"])

        for element in DataPush.DEFAULTS:
            if element not in self.config:
                self.config[element] = DataPush.DEFAULTS[element]
        self.last_update = None
        self.progress_bar = None

    def get_already_uploaded_server(self, ssh, scp):
        print("Looking for .already_uploaded.txt in destination directory")
        _, out, _ = ssh.exec_command("ls -a" + self.config["destination"])
        destination_content = out.readlines()

        temp_dir = tempfile.TemporaryDirectory(prefix="temp_sync", dir="./")
        already_uploaded_server = set()
        if ".already_uploaded.txt" in destination_content:
            print("Found .already_uploaded.txt in destination directory")
            scp.get(self.config["destination"] + ".already_uploaded.txt",
                    temp_dir.name + "/already_uploaded_server.txt")
            with open(temp_dir.name + "/already_uploaded_server.txt") as f:
                already_uploaded_server = set(f.read().splitlines())
        return already_uploaded_server

    def get_already_uploaded_client(self):
        already_uploaded_client = set()
        if os.path.isfile("%s/already_uploaded.txt" % self.config["db_path"]):
            with open("%s/already_uploaded.txt" % self.config["db_path"]) as f:
                already_uploaded_client = already_uploaded_client.union(set(f.read().splitlines()))
        return already_uploaded_client

    def update_already_uploaded_client(self, already_uploaded_client):
        with open("%s/already_uploaded.txt" % self.config["db_path"], "w") as f:
            for entry in already_uploaded_client:
                f.write(entry + "\n")

    def update_already_uploaded_server(self, scp):
        scp.put("%s/already_uploaded.txt" % self.config["db_path"],
                "%s/.already_uploaded.txt" % self.config["destination"])


    def update(self):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        print("Setting missing host key policy")
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        print("Attempting to connect to %s with %s as user and %s as key" % (self.config["ssh_host"], self.config["ssh_user"],
                                                                             self.config["ssh_key"]))
        ssh.connect(self.config["ssh_host"], username=self.config["ssh_user"], key_filename=self.config["ssh_key"])
        print("Connected successfully")
        ssh.exec_command("mkdir -p " + self.config["destination"])


        print("Establishing SCP connection")

        def progress4(filename, size, sent, peername):
            progress = float(sent) / float(size) * 100
            self.progress_bar.n = int(progress)
            self.progress_bar.refresh()

        scp = SCPClient(ssh.get_transport(), progress4=progress4)

        already_uploaded_client = set(self.get_already_uploaded_server(ssh, scp)).union(self.get_already_uploaded_client())

        content = []
        for dir_path, _, files in os.walk(self.config["source"]):
            for file in files:
                ignore_file = False
                for ext in self.config["ignore_extensions"]:
                    if file.endswith(ext):
                        ignore_file = True
                        break
                if not ignore_file:
                    entry = dir_path[len(self.config["source"]) + 1::] + "/" + file
                    content.append(entry)

        nb_uploaded = 0

        for c in content:
            if c not in already_uploaded_client:
                ssh.exec_command('mkdir -p "' + self.config["destination"] + '/' + os.path.dirname(c) + '"')
                print("Uploading " + c)
                self.progress_bar = tqdm(total=100)
                scp.put(self.config["source"] + "/" + c, self.config["destination"] + "/" + c)
                self.progress_bar.close()
                already_uploaded_client.add(c)
                self.update_already_uploaded_client(already_uploaded_client)
                self.update_already_uploaded_server(scp)
                nb_uploaded += 1

        print("Uploaded %d files" % nb_uploaded)


        scp.close()
        self.last_update = time.time()


    def update_loop(self):
        while True:
            curr_time = time.time()
            if self.last_update is None or curr_time - self.last_update > self.config["update_frequency"] * 60:
                try:
                    self.update()
                    print("Finished Update, will check again in %d minutes" % self.config["update_frequency"])
                except Exception as e:
                    print(e)
                    print("Encountered problem, will retry in one minute")
            time.sleep(60)


if __name__ == "__main__":
    print("Starting process")
    if len(sys.argv) != 2:
        raise Exception("Exactly one command line argument is expected")

    ds = DataPush(sys.argv[1])
    ds.update_loop()