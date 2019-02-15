import subprocess
import time
import uuid
import shutil
import pathlib
from datetime import datetime
import argparse
import os
import logging

parser = argparse.ArgumentParser()
req_grp = parser.add_argument_group(title='required argument')
req_grp.add_argument('-w', '--working', required=True, help="Specify the workstation folder (work folder of VirtualBox)")
req_grp.add_argument('-o', '--output', required=True, help="Specify the output folder ")
args = parser.parse_args()

WORKSTATION_FOLDER = pathlib.Path(args.working)
OUTPUT_FOLDER = pathlib.Path(args.output)


FOLDER = "/media/ossama/Data/TesiWorkStation/"

# WORKSTATION_FOLDER = "/media/ossama/Data/VirtualBox_VMs"
# OUTPUT_FOLDER = "/media/ossama/Al Ghofrane"


MEMORY_REQUIRED = 20


def get_device(path):
    output = subprocess.check_output("findmnt -n -o SOURCE --target '{}'".format(path), shell=True)
    output = output.decode('utf-8').split('\n')[0]
    print(output)
    return output


def check_state(vm_uuid):
    vm = list()
    proc = subprocess.check_output("vboxmanage list runningvms", shell=True)
    proc = proc.decode('utf-8')
    row = proc.split('\n')[:-1]
    for x in row:
        y = x.split(' ')[0][1:-1]
        vm.append(y)
    if vm_uuid in vm:
        return True
    return False


def check_memory():
    total_work, used_work, free_work = shutil.disk_usage(WORKSTATION_FOLDER)
    disk_work = get_device(WORKSTATION_FOLDER)
    #print("Total: %d GB" % (total_work // (2 ** 30)))
    #print("Used: %d GB" % (used_work // (2 ** 30)))
    #print("Free: %d GB" % (free_work // (2 ** 30)))
    free_work = free_work // (2 ** 30)
    print(free_work)

    total_output, used_output, free_output = shutil.disk_usage(OUTPUT_FOLDER)
    disk_output = get_device(OUTPUT_FOLDER)
    #print("Total: %d GB" % (total_output // (2 ** 30)))
    #print("Used: %d GB" % (used_output // (2 ** 30)))
    #print("Free: %d GB" % (free_output // (2 ** 30)))
    free_output = free_output // (2 ** 30)
    print(free_output)

    if disk_work != disk_output and free_work >= MEMORY_REQUIRED and free_output >= MEMORY_REQUIRED:

        return True
    elif disk_work == disk_output and free_work >= MEMORY_REQUIRED * 2:
        return True
    return False

def config_logger(logger):
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(filename="/home/ossama/Scrivania/disk.log", mode='w')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s:%(funcName)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)


def main():
    if check_memory():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        config_logger(logger)

        start_time = datetime.now()
        logger.debug(WORKSTATION_FOLDER)
        logger.debug(OUTPUT_FOLDER)
        logger.info('Starting at: {}'.format(start_time))
        case_id=str(uuid.uuid4())
        vm_name = "Deft-"+case_id
        subprocess.run('vboxmanage import ' + FOLDER + 'DeftX.ova --vsys 0 --vmname={}'.format(vm_name), shell=True)
        time.sleep(1)
        logger.debug("activate sniffer")
        subprocess.run("vboxmanage modifyvm {} --nictrace1 on --nictracefile1 '{}/vm_capture.pcap'".format(vm_name, OUTPUT_FOLDER), shell=True)
        logger.debug("start vm")
        subprocess.run('vboxmanage startvm {}'.format(vm_name), shell=True)
        logger.debug("vm started")
        time.sleep(5)
        while check_state(vm_name):
            time.sleep(1)
        logger.debug("vm shutdown")
        logger.debug("deactivate sniffer")
        time.sleep(5)
        subprocess.run("vboxmanage modifyvm {} --nictrace1 off".format(vm_name), shell=True)
        try:
            output = subprocess.check_output(
                "ftkimager {}/{}/DeftX-disk001.vmdk {}/{} --e01 --case-number {}".format(WORKSTATION_FOLDER,vm_name,OUTPUT_FOLDER,vm_name,case_id),
                shell=True)
            logger.debug(output.decode('utf-8'))
        except Exception as e:
            logger.debug(e)
        end_time = datetime.now()
        logger.debug('Duration: {}'.format(end_time - start_time))
        os.system('shutdown now')
    else:
        print("Insufficient free space")


if __name__ == "__main__":
    main()
