#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# NetApp HANA Integration Script
#
# This script allows an SAP HANA administrator to take advantage
# of the data management features offered by the NetApp Cloud Volumes
# Service.
#
# These include application-consistent instant snapshots, restore, and
# cloning.
#
# This is sample code, provided as-is without any maintenance or support, and
# subject to the BSD license.
#
# © 2019, 2020 NetApp, Inc. All Rights Reserved. NETAPP, the NETAPP logo, and the marks 
# listed at http://www.netapp.com/TM are trademarks of NetApp, Inc. in the U.S. and/or 
# other countries. Other company and product names may be trademarks of their respective 
# owners.
#

#
# Google Cloud Installation
#
# 1) pip
#
#     pip --version
#
#   if you don't have pip installed, install it
#
#     python -m ensurepip --default-pip
#
#   to learn more, see:
#
#     https://packaging.python.org/tutorials/installing-packages/
#
# 2) install the Google python client
#
#     pip install google-api-python-client
#
# 3) copy this script to your host and insure it is executable by root
#
# 4) key file
#
#   generate a key file for your service account and copy it to your host
#   in a file called "key.json" - to learn more, see:
#   https://cloud.google.com/solutions/partners/
#       netapp-cloud-volumes-service#cloud_volumes_apis
#
# 5) insure that the userstore contains a key with the permissions required by 
#       the script
#
#   for example, as <SID>adm:
#
#     hdbuserstore set SYSTEM "<hostname>:30013" System "<password>"
#
# 6) configuration file
#
#   optionally, create a config file such as:
#
#     {
#         "project_number": "xxxxxxxxxxxx",
#         "SID": "<SID>",
#         "userstore_key": "BACKUP",
#         "cloud_volumes": ["hana-data", "hana-shared"],
#         "network": "my-vpc"
#     }
#
#   and save it as config.json or <SID>_config.json
#

#
# Dynamically detect the platform we're running on by looking for the
# proper libraries
#
try:
    import google.auth
    import google.auth.transport.requests
    from google.auth import jwt
    from google.oauth2 import service_account
    from google.oauth2 import id_token
except:
    print("Error - proper libraries not found, see installation instructions")
    sys.exit(2)

#
# HANA and OS Interface Functions
#
import os, sys
import argparse
import datetime, time
import subprocess
from subprocess import check_call, check_output, CalledProcessError

#
# Function for running commands
#
# We run commands as root and as the hdbuser, depending on the context.
# 
HANA_NOT_RUNNING = "HANA not running"

def run_command(command, verbose, return_result=False, suppress_error=False, 
    system_id=False):
    try:
        if system_id:
            hdbuser = system_id.lower() + "adm"
            command = ['su', '-', hdbuser, '-c'] + \
                [" ".join(str(x) for x in command)]
        if verbose:
            print("calling: " + " ".join(str(x) for x in command))
            if return_result:
                bytestring = check_output(command)
                output = bytestring.decode('utf-8')
                print(output)
                return output
            else:
                check_call(command)
        else:
            with open(os.devnull, 'w') as DEVNULL:
                if return_result:
                    bytestring = check_output(command, stderr=DEVNULL)
                    output = bytestring.decode('utf-8')
                    return output
                else:
                    check_call(command, stdout=DEVNULL, stderr=DEVNULL)
    except CalledProcessError as ex:
        if suppress_error:
            return HANA_NOT_RUNNING
        print("Error code: " + str(ex.returncode))
        sys.exit(2)

#
# Define SQL prefix
#
HDBSQL = ['hdbsql', '-U']

def is_hana_running(system_id, userstore_key, verbose):
    GET_HANA_STATUS = "SELECT ACTIVE_STATUS FROM SYS.M_DATABASES"

    output = run_command(HDBSQL + [userstore_key] + [GET_HANA_STATUS],
        verbose, True, True, system_id=system_id)
    if output == HANA_NOT_RUNNING:
        return False
    output = output.split()[1]
    if output == '"YES"':
        return True
    print("Error - database in unexpected state: " + output)
    sys.exit(2)

def is_tenant_running(system_id, userstore_key, verbose):
    GET_TENANT_STATUS = "SELECT ACTIVE_STATUS FROM SYS.M_DATABASES WHERE " + \
        "DATABASE_NAME = \"'" + system_id + "'\""

    output = run_command(HDBSQL + [userstore_key] + [GET_TENANT_STATUS], 
        verbose, True, True, system_id=system_id)
    if output == HANA_NOT_RUNNING:
        return False
    output = output.split()[1]
    if output == '"YES"':
        return True
    elif output == '"NO"':
        return False
    print("Error - tenant database in unexpected state: " + output)
    sys.exit(2)

#
# To close an open backup, we first need to have the backup id
#
def get_backup_id(system_id, userstore_key, verbose):
    GET_BACKUP_ID = "SELECT BACKUP_ID FROM M_BACKUP_CATALOG WHERE " + \
        "ENTRY_TYPE_NAME = \"'data snapshot'\" AND STATE_NAME = \"'prepared'\""

    output = run_command(HDBSQL + [userstore_key] + [GET_BACKUP_ID], verbose, 
        return_result=True, system_id=system_id)
    backup_id = output.split()[1]
    if int(backup_id) == 0:
        print("Error: failed to find open snapshot")
        sys.exit(2)
    return backup_id

#
# Open a HANA backup
#
# We use this helper function so it can be called by other functions
#
def open_backup_internal(ebid, system_id, userstore_key, verbose):
    OPEN_BACKUP = "BACKUP DATA FOR FULL SYSTEM CREATE SNAPSHOT COMMENT"

    if ebid:
        comment = "'" + ebid + "'"
    else:
        comment = "'" + datetime.datetime.now().isoformat().replace(":","") + \
            "'"
    output = run_command(HDBSQL + [userstore_key] + [OPEN_BACKUP + " \"" + \
        comment + "\""], verbose, system_id=system_id)

#
# This is the entry point for the command line option
#
def open_backup(ebid, system_id, userstore_key, verbose):
    open_backup_internal(ebid, system_id, userstore_key, verbose)
    backup_id = get_backup_id(system_id, userstore_key, verbose)
    print("Opened backup: " + backup_id)

#
# Close a HANA backup
#
# We use this helper function so it can be called by other functions
#
def close_backup_internal(ebid, system_id, userstore_key, successful, verbose):
    CLOSE_BACKUP = "BACKUP DATA FOR FULL SYSTEM CLOSE SNAPSHOT BACKUP_ID"

    backup_id = get_backup_id(system_id, userstore_key, verbose)
    if successful:
        if ebid:
            comment = "\"'" + ebid + "'\""
        else:
            comment = "\"'NetApp snapshot successful'\""
        run_command(HDBSQL + [userstore_key] + [CLOSE_BACKUP] + [backup_id] + \
            ["SUCCESSFUL"] + [comment], verbose, system_id=system_id)
    else:
        comment = "\"'NetApp snapshot creation timed out'\""
        run_command(HDBSQL + [userstore_key] + [CLOSE_BACKUP] + [backup_id] + \
            ["UNSUCCESSFUL"] + [comment], verbose, system_id=system_id)
    return backup_id

#
# This is the entry point for the command line option
#
def close_backup(ebid, system_id, userstore_key, verbose):
    backup_id = close_backup_internal(ebid, system_id, userstore_key, True, \
        verbose)
    print("Closed backup: " + backup_id)

def stop_hana(system_id, userstore_key, verbose):
    STOP_HANA = ["HDB", "stop"]
    STOP_TENANT = "ALTER SYSTEM STOP DATABASE " + system_id

    if is_tenant_running(system_id, userstore_key, verbose):
        run_command(HDBSQL + [userstore_key] + [STOP_TENANT], verbose, 
            system_id=system_id)
        print("Stopped Tenant Database")
    else:
        print("Tenant Database not running")
    run_command(STOP_HANA, verbose, system_id=system_id)
    print("Stopped HANA")

#
# This helper function handles the OS-related restore steps
#
def restore_internal(mount_point, snapshot, verbose):
    RSYNC = ["rsync", "-axhv", "--delete", "--progress"]

    if not mount_point:
        print("Error - volume '" + cloud_volume + "' not found: " +
            "insure the volume is mounted on the host where this command is " +
            "executed")
        sys.exit(2)

    source = mount_point + "/.snapshot/" + snapshot + "/"
    destination = mount_point + "/"

    if not os.path.exists(source):
        print("Error - snapshot '" + snapshot + "' not found")
        sys.exit(2)

    run_command(RSYNC + [source] + [destination], verbose)
    print("Restore complete")

import json
import requests

DEFAULT_SERVICE_ACCOUNT_FILE_NAME = 'key.json'
DEFAULT_CONFIG_FILE_NAME = 'config.json'
DEFAULT_USERSTORE_KEY = 'SYSTEM'
DEFAULT_TIMEOUT = 5

#
# Google Cloud version of the CVS API
#

AUDIENCE = 'https://cloudvolumesgcp-api.netapp.com'

class CVS4GC():

    #
    # Dynamically lookup the project number
    # - only works if the service account only has access to one project or 
    # the right project comes back from the server first
    #
    def get_project_number(self, key_file, verbose):
        GC_AUDIENCE = 'https://cloudresourcemanager.googleapis.com'

        gc_auth = self.get_auth(key_file, verbose, GC_AUDIENCE + "/")
        get_url = GC_AUDIENCE + "/v1/projects/"
        r = requests.get(get_url, headers=gc_auth)
        if r.status_code != 200:
            if verbose:
                print("Error retrieving project number from " + GC_AUDIENCE + \
                    " - " + r.text)
        else:
            r_dict = r.json()
            if r_dict.get('projects'):
                projects = r_dict.get('projects')
                project = projects[0]
                project_number = project['projectNumber']
        return project_number

    #
    # Read parameters out of the config file
    #
    def get_config(self, config_file, key_file, system_id, userstore_key, 
        cloud_volumes, verbose):
        # command line argument takes precedence over file name based on SID
        # which takes precedence over  the default file name
        if not config_file:
            if system_id:
                config_file = system_id + "_" + DEFAULT_CONFIG_FILE_NAME
            else:
                config_file = DEFAULT_CONFIG_FILE_NAME

        if verbose:
            print("Loading configuration from '" + config_file + "'")

        try:
            with open(config_file) as file:
                config = json.load(file)
        except:
            if verbose:
                print("File '" + config_file + "' not found or failed to load")
            project_number = self.get_project_number(key_file, verbose)
            if not userstore_key:
                userstore_key = DEFAULT_USERSTORE_KEY
            if cloud_volumes:
                cloud_volumes = cloud_volumes.split(",")
            return project_number, system_id, userstore_key, cloud_volumes, ""

        project_number = config.get("project_number")
        if not project_number:
            project_number = self.get_project_number(key_file, verbose)
        if not system_id:
            system_id = config.get("SID")
        if not userstore_key:
            userstore_key = config.get("userstore_key")
            if not userstore_key:
                userstore_key = DEFAULT_USERSTORE_KEY
        if not cloud_volumes:
            cloud_volumes = config.get("cloud_volumes")
        else:
            cloud_volumes = cloud_volumes.split(",")
        network = config.get("network")

        return project_number, system_id, userstore_key, cloud_volumes, network

    #
    # Get the credentials from the key file and contruct the auth
    # header
    #
    def get_auth(self, key_file, verbose, audience=AUDIENCE):
        if not key_file:
            key_file = DEFAULT_SERVICE_ACCOUNT_FILE_NAME

        try:
            service_credentials = \
                service_account.Credentials.from_service_account_file(key_file)
        except:
            if verbose:
                print("File '" + key_file + "' not found or failed to load")
            return ""

        jwt_credentials = jwt.Credentials.from_signing_credentials(
            service_credentials, audience=audience)
        request = google.auth.transport.requests.Request()
        jwt_credentials.refresh(request)
        auth_token = jwt_credentials.token

        auth = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + auth_token.decode('utf-8')
        }
        return auth

    #
    # cloud_volume could be a path to a mount point or the name of a
    # cloud volume, in either case, the volumes attributes are returned 
    #
    def get_volume(self, cloud_volume, project_number, auth, verbose):
        cloud_volume_candidate = run_command(["/bin/findmnt", cloud_volume, 
            "-no", "SOURCE"], False, return_result=True, suppress_error=True)
        if cloud_volume_candidate != HANA_NOT_RUNNING:
            value = (cloud_volume_candidate.split("/")[1]).strip("\n")
            key = "creationToken"
        else:
            value = cloud_volume
            key = "name"

        get_url = AUDIENCE + "/v2/projects/" + project_number + \
            "/locations/-/Volumes"
        r = requests.get(get_url, headers=auth)
        if r.status_code != 200:
            print("Error listing volumes - '" + r.text)
            sys.exit(2)

        r_dict = r.json()
        for vol in r_dict:
            if vol[key] == value:
                if verbose:
                    print("Found " + cloud_volume + " with volumeId = " + \
                        vol["volumeId"])
                return vol

    #
    # Find a mount point on the host for a cloud volume
    #
    def get_mount_point(self, cloud_volume, project_number, auth, verbose):
        cloud_volume_candidate = run_command(["/bin/findmnt", cloud_volume, \
            "-no", "SOURCE"], False, return_result=True, suppress_error=True)
        if cloud_volume_candidate != HANA_NOT_RUNNING:
            return cloud_volume

        if not project_number:
            print("Error - \"project_number\" unknown, specify in " + \
                "configuration file")
            sys.exit(2)
        if not auth:
            print("Error - KEY_FILE not found, specify path")
            sys.exit(2)

        vol = self.get_volume(cloud_volume, project_number, auth, verbose)
        if not vol:
            print("Volume '" + cloud_volume + "' not found")
            sys.exit(2)

        export = vol['creationToken']
        # the volume must be mounted by our host in order to restore from it
        with open('/proc/mounts', 'r') as file:
            for line in file.readlines():
                filesystem = line.split()[0]
                if len(filesystem.split("/")) == 2 and \
                    filesystem.split("/")[1] == export:
                    mount_point = line.split()[1]
                    break

        if not mount_point:
            print("Volume '" + cloud_volume + \
                "' not found - insure the volume is mounted on the host")
            sys.exit(2)

        return mount_point

    #
    # Lookup the id for a snapshot
    #
    def get_snapshot_id(self, project_number, region, volume_id, snapshot_name, 
        auth, verbose):
        get_url = AUDIENCE + "/v2/projects/" + project_number + \
            "/locations/" + region + "/Volumes/" + volume_id + "/Snapshots"
        r = requests.get(get_url, headers=auth)
        if r.status_code != 200:
            print("Error listing snapshots - '" + r.text)
            sys.exit(2)

        r_dict = r.json()
        for snapshot in r_dict:
            if snapshot["name"] == snapshot_name:
                return snapshot["snapshotId"]

    #
    # Go through a list of snapshots to make sure they all exist and none
    # have a snapshot of the given name
    #
    def validate_cloud_volumes(self, cloud_volumes, project_number, auth,
        snapshot_name, verbose):
        volumes = {}
        for cloud_volume in cloud_volumes:
            vol = self.get_volume(cloud_volume, project_number, auth, verbose)
            if not vol:
                print("Error - volume '" + cloud_volume + "' not found")
                sys.exit(2)
            region = vol["region"]
            volume_id = vol["volumeId"]
            snapshot_id = self.get_snapshot_id(project_number, region, 
                volume_id, snapshot_name, auth, verbose)
            if snapshot_id:
                print("Error - snapshot '" + snapshot_name + "' already exists")
                sys.exit(2)
            volumes.update([(cloud_volume, vol)])

        return volumes

    #
    # Create a snapshot of each cloud volume in the list 
    # - assumes the cloud volume list has already been validated
    #
    def create_snapshot_internal(self, volumes, cloud_volumes, project_number,
        auth, snapshot_name, verbose):

        start_time = datetime.datetime.now()
        for cloud_volume in cloud_volumes:
            vol = volumes[cloud_volume]
            post_url = AUDIENCE + "/v2/projects/" + project_number + \
                "/locations/" + vol["region"] + "/Volumes/" + \
                vol["volumeId"] + "/Snapshots"
            payload = {
                "name": snapshot_name
            }
            requests.post(post_url, headers=auth, json=payload)

        # wait for snapshots to be created
        for x in range(DEFAULT_TIMEOUT):
            pending_volumes = list(cloud_volumes)
            for cloud_volume in pending_volumes:
                vol = volumes[cloud_volume]
                snapshot_id = self.get_snapshot_id(project_number, 
                    vol["region"], vol["volumeId"], snapshot_name, auth, 
                    False)
                if snapshot_id:
                    cloud_volumes.remove(cloud_volume)
            if not cloud_volumes:
                break
            time.sleep(1)
        elapsed = datetime.datetime.now() - start_time

        if cloud_volumes:
            print("Error - snapshot '" + snapshot_name + \
                "' not created after " + str(elapsed.total_seconds()) + \
                " seconds on the following volume(s): " + \
                ", ".join(pending_volumes))
            return False
        else:
            print("Created snapshot '" + snapshot_name + "' in " + \
                str(elapsed.total_seconds()) + " seconds")
            return True

    #
    # Create a snapshot of each cloud volume in the list
    #
    def create_snapshot(self, cloud_volumes, snapshot_name, system_id, 
        auth, project_number, verbose):

        # validate arguments
        if not project_number:
            print("Error - \"project_number\" unknown, specify in " + \
                "configuration file")
            sys.exit(2)
        if not snapshot_name:
            # snapshot names may not contain ":" characters, so remove
            snapshot_name = datetime.datetime.now().isoformat().replace(":","")
        if not cloud_volumes:
            print("Error - no cloud volumes specified, specify with " + \
                "--cloud-volumes or in configuration file")
            sys.exit(2)
        if verbose:
            print("Preparing to create snapshot of: " + \
                ", ".join(cloud_volumes))
        volumes = self.validate_cloud_volumes(cloud_volumes, project_number,
            auth, snapshot_name, verbose)

        self.create_snapshot_internal(volumes, cloud_volumes, project_number,
            auth, snapshot_name, verbose)

    #
    # Create an application-consistent snapshot of a HANA database
    #
    def hana_backup(self, cloud_volumes, snapshot_name, system_id, 
        userstore_key, auth, project_number, verbose):

        # validate arguments
        if not project_number:
            print("Error - \"project_number\" unknown, specify in " + \
                "configuration file")
            sys.exit(2)
        if not snapshot_name:
            # snapshot names may not contain ":" characters, so remove
            snapshot_name = datetime.datetime.now().isoformat().replace(":","")
        if not cloud_volumes:
            print("Error - no cloud volumes specified, specify with " + \
                "--cloud-volumes or in configuration file")
            sys.exit(2)
        if verbose:
            print("Preparing to create snapshot of: " + \
                ", ".join(cloud_volumes))
        if not system_id:
            print("Error - System ID unknown, specify with --SID or " + \
                "in configuration file")
            sys.exit(2)
        volumes = self.validate_cloud_volumes(cloud_volumes, project_number,
            auth, snapshot_name, verbose)

        # open HANA backup
        open_backup_internal(snapshot_name, system_id, userstore_key, verbose)

        successful = self.create_snapshot_internal(volumes, cloud_volumes, 
            project_number, auth, snapshot_name, verbose)

        # close HANA backup
        close_backup_internal(snapshot_name, system_id, userstore_key, \
            successful, verbose)

    #
    # Copy the contents of a snapshot into the active filesystem
    # - the database must be stopped first
    #
    def restore(self, cloud_volume, snapshot, system_id, userstore_key, auth, 
        project_number, verbose):
        if is_hana_running(system_id, userstore_key, verbose):
            print("Error - database must be stopped before it can be restored")
            sys.exit(2)

        mount_point = self.get_mount_point(cloud_volume, project_number,
            auth, verbose)

        restore_internal(mount_point, snapshot, verbose)

    #
    # Provision a new cloud volume which is a clone of the snapshot of
    # another cloud volume
    #   
    def clone(self, cloud_volume, snapshot, volume_name, export_path, cidr, 
        system_id, userstore_key, auth, project_number, verbose):

        if not export_path:
            export_path = volume_name
        if not cidr:
            cidr = "0.0.0.0/0"

        # validate arguments
        if not project_number:
            print("Error - \"project_number\" unknown, specify in " + \
                "configiuration file")
            sys.exit(2)
        if not network:
            print("Error - \"network\" unknown, specify in " + \
                "configiuration file")
            sys.exit(2)
        if not volume_name:
            print("Error - VOLUME_NAME is a required argument")
            sys.exit(2)
        vol = self.get_volume(volume_name, project_number, auth, verbose)
        if vol:
            print("Error - volume '" + volume_name + "' already exists")
            sys.exit(2)
        if not cloud_volume:
            print("Error - CLOUD_VOLUME is a required argument")
            sys.exit(2)
        vol = self.get_volume(cloud_volume, project_number, auth, verbose)
        if not vol:
            print("Error - volume '" + cloud_volume + "' not found")
            sys.exit(2)
        region = vol["region"]
        volume_id = vol["volumeId"]
        if not snapshot:
            print("Error - SNAPSHOT is a required argument")
            sys.exit(2)
        snapshot_id = self.get_snapshot_id(project_number, region, volume_id, 
            snapshot, auth, verbose)
        if not snapshot_id:
            print("Error - snapshot '" + snapshot + "' not found")
            sys.exit(2)

        post_url = AUDIENCE + "/v2/projects/" + project_number + \
            "/locations/" + region + "/Volumes"

        payload = {
            "name": volume_name,
            "creationToken": export_path,
            "region": region,
            "serviceLevel": vol["serviceLevel"],
            "quotaInBytes": vol["quotaInBytes"],
            "network": "projects/" + project_number + "/global/networks/" + \
                network,
            "snapReserve": vol["snapReserve"],
            "protocolTypes": vol["protocolTypes"],
            "exportPolicy": {
                "rules": [
                    {
                        "allowedClients": cidr,
                        "access": "ReadWrite"
                    }
                    ]
            },
            "snapshotId": snapshot_id
        }

        start_time = datetime.datetime.now()
        r = requests.post(post_url, json=payload, headers=auth)
        if r.status_code != 201 and r.status_code != 202:
            print("Error creating clone - '" + r.text)
            sys.exit(2)

        results = r.json()
        elapsed = datetime.datetime.now() - start_time

        if results.get("message"):
            print("Error creating clone - '" + results['message'])
            sys.exit(2)
        if verbose:
            print("Waiting for clone '" + volume_name + "'")

        # Wait for clone to be available - 10 times the default timeout
        for x in range(DEFAULT_TIMEOUT * 10):
            try:
                vol = self.get_volume(volume_name, project_number, auth, False)
                if vol["lifeCycleState"] != 'creating':
                    break
            except:
                pass
            time.sleep(1)

        if vol["lifeCycleState"] != 'available':
            print("Error - clone failed to initialize: '" + \
                vol["lifeCycleStateDetails"] + "'")
        else:
            print("Created clone '" + volume_name + "' in " + \
                str(elapsed.total_seconds()) + " seconds")

    #
    # List the snapshots of a cloud volume, including their names, creation
    # date and how much storage is "locked" in them.
    #
    def list_snapshots(self, cloud_volume, system_id, auth, project_number, 
        verbose):

        # validate arguments
        if not project_number:
            print("Error - \"project_number\" unknown, specify in " + \
                "configiuration file")
            sys.exit(2)
        if not cloud_volume:
            print("Error - CLOUD_VOLUME is a required argument")
            sys.exit(2)
        vol = self.get_volume(cloud_volume, project_number, auth, verbose)
        if not vol:
            print("Error - volume '" + cloud_volume + "' not found")
            sys.exit(2)
        region = vol["region"]
        volume_id = vol["volumeId"]

        get_url = AUDIENCE + "/v2/projects/" + project_number + \
            "/locations/" + region + "/Volumes/" + volume_id + \
            "/Snapshots"
        r = requests.get(get_url, headers=auth)
        if r.status_code != 200:
            print("Error listing snapshots - '" + r.text)
            sys.exit(2)

        # use a standard library to format the table
        r_dict = r.json()
        row_format ="{:>30} {:>30} {:>10}"
        print(row_format.format("Name", "Created", "Used MB"))
        for snapshot in r_dict:
            # convert bytes to MB for readability
            print(row_format.format(snapshot['name'], snapshot['created'],
                snapshot['usedBytes'] / (1000 * 1000)))

    #
    # Delete a snapshot or all snapshots before and including a snapshot
    # - snapshots which are the bases of clones cannot be deleted
    #
    def delete_snapshot(self, cloud_volume, snapshot, all_previous, system_id, 
        auth, project_number, verbose):

        # validate arguments
        if not project_number:
            print("Error - \"project_number\" unknown, specify in " + \
                "configiuration file")
            sys.exit(2)
        if not cloud_volume:
            print("Error - CLOUD_VOLUME is a required argument")
            sys.exit(2)
        if not snapshot:
            print("Error - SNAPSHOT is a required argument")
            sys.exit(2)

        vol = self.get_volume(cloud_volume, project_number, auth, verbose)
        if not vol:
            print("Error - volume '" + cloud_volume + "' not found")
            sys.exit(2)
        region = vol["region"]
        volume_id = vol["volumeId"]
        snapshot_id = self.get_snapshot_id(project_number, region, volume_id,
            snapshot, auth, verbose)
        if not snapshot_id:
            print("Error - snapshot '" + snapshot + "' not found")
            sys.exit(2)

        # get a list of all snapshot we might want to delete
        if all_previous:
            get_url = AUDIENCE + "/v2/projects/" + project_number + \
                "/locations/" + region + "/Volumes/" + volume_id + \
                "/Snapshots"
            r = requests.get(get_url, headers=auth)
            if r.status_code != 200:
                print("Error listing snapshots - '" + r.text)
                sys.exit(2)
            deletion_list = r.json()
        else:
            get_url = AUDIENCE + "/v2/projects/" + project_number + \
                "/locations/" + region + "/Volumes/" + volume_id + \
                "/Snapshots/" + snapshot_id
            r = requests.get(get_url, headers=auth)
            if r.status_code != 200:
                print("Error listing snapshot - '" + r.text)
                sys.exit(2)
            tmp = r.json()
            deletion_list = [tmp]

        # get the date before which we will delete all snapshots        
        for snap in deletion_list:
            if snap['snapshotId'] == snapshot_id:
                created = snap['created']
                if verbose and all_previous:
                    print("Delete all snapshots before " + created)
        if not created:
            print("Error - failed to find creation date of requested " +
                "snaphot: " + snapshot)
            sys.exit(2)

        # request deletions in parallel
        start_time = datetime.datetime.now()
        deletion_candidates = list(deletion_list)
        for snap in deletion_candidates:
            if snap['created'] <= created:
                if verbose:
                    print("Delete snapshot: " + snap['name'])
                delete_url = AUDIENCE + "/v2/projects/" + project_number + \
                    "/locations/" + region + "/Volumes/" + volume_id + \
                    "/Snapshots/" + snap['snapshotId']
                r = requests.delete(delete_url, headers=auth)
                if r.status_code != 200 and r.status_code != 202:
                    print("Error deleting snapshot - '" + r.text)
            else:
                deletion_list.remove(snap)

        # wait for snapshot to be deleted
        for x in range(DEFAULT_TIMEOUT):
            pending_list = list(deletion_list)
            for snap in pending_list:
                snapshot_id = self.get_snapshot_id(project_number, region, 
                    volume_id, snap['name'], auth, verbose)
                if not snapshot_id:
                    print("Snapshot '" + snap['name'] + "' deleted")
                    deletion_list.remove(snap)
            if not deletion_list:
                break
            time.sleep(1)
        elapsed = datetime.datetime.now() - start_time

        if deletion_list:
            print("Error - not all snapshots deleted in " + \
                str(elapsed.total_seconds()) + " seconds")
            sys.exit(2)
        if all_previous:
            print("All snapshots before and including '" + snapshot + \
                "' deleted in " + str(elapsed.total_seconds()) + " seconds")

if __name__ == "__main__":
    # create platform-specific object
    CVS = CVS4GC()

    parser = argparse.ArgumentParser()
    parser.add_argument("--hana-backup", "-b", action="store_true",
        help="Usage: ntaphana --hana-backup \
        [--cloud-volumes CLOUD_VOLUMES] [--backup-name BACKUP_NAME] \
        [--SID SID] [--userstore-key USERSTORE_KEY] \
        [--key-file KEY_FILE] [--config-file CONFIG_FILE] [--verbose] \
        create a consistent backup of a HANA database and a backing snapshot \
        of the cloud volume or volumes")
    parser.add_argument("--create-snapshot", "-s", action="store_true",
        help="Usage: ntaphana --create-snapshot \
        [--cloud-volumes CLOUD_VOLUMES] [--snapshot-name SNAPSHOT_NAME] \
        [--SID SID] [--key-file KEY_FILE] \
        [--config-file CONFIG_FILE] [--verbose] \
        create a snapshot of the cloud volume or volumes")
    parser.add_argument("--open-backup", "-o", action="store_true",
        help="Usage: ntaphana --open-backup [--EBID EBID] \
        [--SID SID] [--userstore-key USERSTORE_KEY] \
        [--config-file CONFIG_FILE] [--verbose] start a backup of HANA and \
        set the EBID as the comment")
    parser.add_argument("--close-backup", "-e", action="store_true",
        help="Usage: ntaphana --close-backup [--EBID EBID] \
        [--SID SID] [--userstore-key USERSTORE_KEY] \
        [--config-file CONFIG_FILE] [--verbose] close a backup of HANA and \
        set the EBID as the comment")
    parser.add_argument("--restore", "-r", action="store_true",
        help="Usage: ntaphana --restore --cloud-volume CLOUD_VOLUME \
        --snapshot SNAPSHOT [--SID SID] [--userstore-key USERSTORE_KEY] \
        [--key-file KEY_FILE] [--config-file CONFIG_FILE] [--verbose] \
        restore data by copying from a snapshot into the active filesystem; \
        database must be stopped first and this command must be run on the \
        host where the database is running")
    parser.add_argument("--clone", "-l", action="store_true",
        help="Usage: ntaphana --clone --cloud-volume CLOUD_VOLUME \
        --snapshot SNAPSHOT --volume-name VOLUME_NAME \
        [--export-path EXPORT_PATH] [--CIDR CIDR] [--SID SID] \
        [--userstore-key USERSTORE_KEY] [--key-file KEY_FILE] \
        [--config-file CONFIG_FILE] [--verbose] \
        create a volume from a snapshot; specify the volume and \
        snapshot to be cloned and the name of the new volume")
    parser.add_argument("--list-snapshots", "-L", action="store_true",
        help="Usage: ntaphana --list-snapshots --cloud-volume CLOUD_VOLUME \
        [--SID SID] [--key-file KEY_FILE] [--config-file CONFIG_FILE] \
        [--verbose] \
        list the snapshots of a cloud volume")
    parser.add_argument("--delete-snapshot", "-x", action="store_true",
        help="Usage: ntaphana --delete-snapshot --cloud-volume CLOUD_VOLUME \
        --snapshot SNAPSHOT --all-previous [--SID SID] [--key-file KEY_FILE] \
        [--config-file CONFIG_FILE] [--verbose] \
        permanently delete a snapshot of a cloud volume; use the \
        'all-previous' flag with caution")
    parser.add_argument("--cloud-volumes", "-c", help="the name of a mount \
        point or a cloud volume or a comma-separated list")
    parser.add_argument("--backup-name", "-p", help="both the \
        external backup id and the name of the snapshot")
    parser.add_argument("--SID", "-i", help="the System ID of a HANA database")
    parser.add_argument("--userstore-key", "-y", help="a userstore key with \
        the permissions required to administer HANA")
    parser.add_argument("--key-file", "-k", help="the name of a file \
        containing credentials appropriate to the cloud provider; a path may \
        be specified; by default, a file named \"key.json\" in the local \
        directory will be used")
    parser.add_argument("--config-file", "-f", help="the name of a file \
        which may contain a project number, SID, hdbuserstore key, list \
        of cloud volumes or virtual private cloud; by default, the local \
        directory will be searched for a file named \"SID_config.json\" \
        or \"config.json\", where SID is the System ID of the HANA database")
    parser.add_argument("--verbose", "-v", action="store_true",
        help="show execution details")
    parser.add_argument("--snapshot-name", "-n", help="name of the snapshot")
    parser.add_argument("--EBID", "-d", help="external backup id")
    parser.add_argument("--cloud-volume", "-g", help="the name of a mount \
        point or a cloud volume")
    parser.add_argument("--snapshot", "-j", help="the name of a snapshot")
    parser.add_argument("--volume-name", "-u", help="name of the volume to \
        create")
    parser.add_argument("--export-path", "-a", help="export path of the clone; \
        if not specified, the name of the new volume will be used as the \
        export path")
    parser.add_argument("--CIDR", "-z", help="CIDR format describing hosts to \
        export to with full permission; if not specified 0.0.0.0/0 will be \
        used")
    parser.add_argument("--all-previous", "-P", action="store_true",
        help="delete all previous snapshots as well as the specified snapshot; \
        use with caution")
    args = parser.parse_args()

    # load the authentication headers from the key file
    auth = CVS.get_auth(args.key_file, args.verbose)
    # load the configuration from the config file
    project_number, system_id, userstore_key, cloud_volumes, network = \
        CVS.get_config(args.config_file, args.key_file, args.SID, 
        args.userstore_key, args.cloud_volumes, args.verbose)

    if args.hana_backup:
        CVS.hana_backup(cloud_volumes, args.backup_name, system_id, 
            userstore_key, auth, project_number, args.verbose)
    elif args.create_snapshot:
        CVS.create_snapshot(cloud_volumes, args.snapshot_name, system_id, 
            auth, project_number, args.verbose)
    elif args.open_backup:
        open_backup(args.EBID, system_id, userstore_key, args.verbose)
    elif args.close_backup:
        close_backup(args.EBID, system_id, userstore_key, args.verbose)
    elif args.restore:
        CVS.restore(args.cloud_volume, args.snapshot, system_id, userstore_key,
            auth, project_number, args.verbose)
    elif args.clone:
        CVS.clone(args.cloud_volume, args.snapshot, args.volume_name,
            args.export_path, args.CIDR, system_id, userstore_key, 
            auth, project_number, args.verbose)
    elif args.list_snapshots:
        CVS.list_snapshots(args.cloud_volume, system_id, auth, project_number, 
            args.verbose)
    elif args.delete_snapshot:
        CVS.delete_snapshot(args.cloud_volume, args.snapshot, args.all_previous,
            system_id, auth, project_number, args.verbose)
    else:
        print("Error: specify --hana-backup, --create-snapshot, " + \
            "--open-backup, --close-backup, --restore, --clone, " + \
            "--list-snapshots, or --delete-snapshot")

