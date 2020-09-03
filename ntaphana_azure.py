#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# NetApp HANA Integration Script
#
# This script allows an SAP HANA administrator to take advantage
# of the data management features offered by the Azure NetApp Files Service.
#
# These include application-consistent instant snapshots, restore, and
# cloning.
#
# This is sample code, provided as-is without any maintenance or support, and
# subject to the Apache License 2.0.
#
# © 2019, 2020 NetApp, Inc. All Rights Reserved. NETAPP, the NETAPP logo, and 
# the marks listed at http://www.netapp.com/TM are trademarks of NetApp, Inc. 
# in the U.S. and/or other countries. Other company and product names may be 
# trademarks of their respective owners.
#

#
# Azure Installation
#
# 1) pip
#
#     pip3 --version
#
#   if you don't have pip installed, install it
#
#     python3 -m ensurepip --default-pip
#
#   to learn more, see:
#
#     https://packaging.python.org/tutorials/installing-packages/
#
# 2) install the ANF components of the Azure SDK
#
#     pip3 install azure.mgmt.netapp
#
# 3) copy this script to your host and insure it is executable by root
#
#     python3 is required
#
# 4) when onboarded to Azure, you received credentials with the following or
#    similar format:
#
#     {
#         "appId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
#         "displayName": "SAP",
#         "name": "http://SAP",
#         "password": "XXX",
#         "tenant": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
#     }
#
#   save these credentials in a file called "key.json"
#
# 5) insure that the userstore contains a key with the permissions required by
#       the script
#
#   for example, as <SID>adm:
#
#     hdbuserstore set BACKUP "<hostname>:30013" System "<password>"
#
# 6) configuration file
#
#   optionally, create a config file such as:
#
#     {
#         "SID": "<SID>",
#         "userstore_key": "BACKUP",
#         "cloud_volumes": ["hana-data", "hana-shared"],
#         "subscription_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
#     }
#
#   and save it as config.json or <SID>_config.json
#

#
# HANA and OS Interface Functions
#
import os, sys
import argparse
import datetime, time
import subprocess
from subprocess import check_call, check_output, CalledProcessError

#
# Dynamically detect the platform we're running on by looking for the
# proper libraries
#
try:
    from azure.common.credentials import ServicePrincipalCredentials
    from azure.mgmt.subscription import SubscriptionClient
    from azure.mgmt.netapp import AzureNetAppFilesManagementClient
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.netapp.models import Snapshot
    from azure.mgmt.netapp.models import Volume
except:
    print("Error - expected libraries not found, see installation instructions")
    sys.exit(2)

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
        comment = "'" + create_snapshot_name() + "'"
    output = run_command(HDBSQL + [userstore_key] + [OPEN_BACKUP + " \"" + \
        comment + "\""], verbose, system_id=system_id)

#
# This is the entry point for the command line option
#
def open_backup(ebid, system_id, userstore_key, verbose):
    if not system_id:
        print("Error - no SID specified, specify with " + \
                "--SID or in configuration file")
        sys.exit(2)

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
    if not system_id:
        print("Error - no SID specified, specify with " + \
                "--SID or in configuration file")
        sys.exit(2)

    backup_id = close_backup_internal(ebid, system_id, userstore_key, True, \
        verbose)
    print("Closed backup: " + backup_id)

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

#
# Generate a default snapshot name
#
def create_snapshot_name():
    # snapshot names may not contain ":" or "." characters, so remove
    date = datetime.datetime.now().isoformat()
    snapshot_name = date.replace(":","-").replace(".","-")
    return snapshot_name

import json

DEFAULT_SERVICE_ACCOUNT_FILE_NAME = 'key.json'
DEFAULT_CONFIG_FILE_NAME = 'config.json'
DEFAULT_USERSTORE_KEY = 'SYSTEM'
DEFAULT_TIMEOUT = 5

#
# Azure API Integration
#
# We implement snapshot, restore and clone. 
#

class ANF():

    #
    # Get the credentials from the key file and construct the
    # ServicePrincipalCredentials
    #
    def get_auth(self, key_file, verbose):
        if not key_file:
            key_file = DEFAULT_SERVICE_ACCOUNT_FILE_NAME

        try:
            with open(key_file) as file:
                service_principal = json.load(file)
        except:
            print("File '" + key_file + "' not found or failed to load")
            return ""

        credentials = ServicePrincipalCredentials(
            client_id = service_principal.get("appId"),
            secret = service_principal.get("password"),
            tenant = service_principal.get("tenant")
        )

        return credentials

    #
    # Lookup the subscription id
    # - if there are more than one, warn the user and use the first
    #
    def get_subscription_id(self, key_file, verbose):
        credentials = self.get_auth(key_file, verbose)
        subscription_id = ""
        
        subscription_client = SubscriptionClient(credentials)
        for item in subscription_client.subscriptions.list():
            if not subscription_id:
                subscription_id = item.subscription_id
            elif verbose:
                print("You have more than one subscription id, " + \
                    "using the first one returned; consider setting " + \
                    "subscription_id in configuration file")

        return subscription_id

    #
    # Read parameters out of the config file
    #
    def get_config(self, config_file, key_file, system_id, userstore_key,
        cloud_volumes, verbose):
        # command line argument takes precedence over file name based on SID
        # which takes precedence over the default file name
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
            subscription_id = self.get_subscription_id(key_file, verbose)
            if not userstore_key:
                userstore_key = DEFAULT_USERSTORE_KEY
            if cloud_volumes:
                cloud_volumes = cloud_volumes.split(",")
            return subscription_id, system_id, userstore_key, cloud_volumes, ""

        subscription_id = config.get("subscription_id")
        if not subscription_id:
            subscription_id = self.get_subscription_id(key_file, verbose)
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

        return subscription_id, system_id, userstore_key, cloud_volumes, ""

    #
    # examine an untyped member of a resource group and return true if it is a
    # volume
    #
    def is_volume(self, member, volume_name):
        # a volume id has the following format:
        # /subscriptions/a6789047-29d4-4ce0-89e6-dfbbe5ad95e0/resourceGroups/
        # techmarketing.rg/providers/Microsoft.NetApp/netAppAccounts/
        # techmarketing/capacityPools/tmpool01/volumes/myVolumeName
        path = member.id.split('/')

        if (len(path) == 13) and (path[11] == 'volumes') and \
            (path[12] == volume_name):
            return True

        return False

    #
    # cloud_volume could be a path to a mount point or the name of a
    # cloud volume, in either case, the volume's attributes are returned
    #
    def get_volume(self, cloud_volume, subscription_id, credentials, verbose):
        cloud_volume_candidate = run_command(["/bin/findmnt", cloud_volume,
            "-no", "SOURCE"], False, return_result=True, suppress_error=True)
        if cloud_volume_candidate != HANA_NOT_RUNNING:
            volume_name = (cloud_volume_candidate.split("/")[1]).strip("\n")
        else:
            volume_name = cloud_volume
        generic_volume = ""

        resource_client = ResourceManagementClient(credentials, subscription_id)

        # we iterate through every member of every resource group the user has
        # access to and look for a matching volume
        for item in resource_client.resource_groups.list():
            for member in resource_client.resources.list_by_resource_group(
                item.name):
                if self.is_volume(member, volume_name):
                    if generic_volume:
                        print("Error - Found more than one volume named '" + \
                            cloud_volume + "'")
                        sys.exit(2)
                    generic_volume = member
                    resource_group, netapp_account, capacity_pool = \
                        self.parse_volume_id(generic_volume)
                    if verbose:
                        print("Found volume '" + member.name + "'")

        if not generic_volume:
            return ""

        anf_client = AzureNetAppFilesManagementClient(credentials,
            subscription_id)
        volume = anf_client.volumes.get(resource_group, netapp_account,
            capacity_pool, volume_name)

        if not volume:
            print("Volume '" + cloud_volume + "' not found")
            sys.exit(2)

        return volume

    #
    # Parse the volume's identifier and return the:
    # - resource group
    # - netapp account
    # - capacity pool
    #
    def parse_volume_id(self, volume):
        path = volume.id.split('/')
        return path[4], path[8], path[10]

    #
    # Lookup the id for a snapshot
    #
    def get_snapshot_id(self, subscription_id, credentials, volume, 
        snapshot_name, verbose):
        anf_client = AzureNetAppFilesManagementClient(credentials,
            subscription_id)

        resource_group, netapp_account, capacity_pool =  \
            self.parse_volume_id(volume)
        cloud_volume = volume.creation_token

        try:
            snapshot = anf_client.snapshots.get(resource_group, netapp_account,
                capacity_pool, cloud_volume, snapshot_name)
            return snapshot.snapshot_id
        except:
            return ""

    #
    # Go through a list of volumes to make sure they all exist and none
    # have a snapshot of the given name
    #
    def validate_cloud_volumes(self, cloud_volumes, subscription_id, 
        credentials, snapshot_name, verbose):
        volumes = {}
        for cloud_volume in cloud_volumes:
            vol = self.get_volume(cloud_volume, subscription_id, credentials, 
                verbose)
            if not vol:
                print("Error - volume '" + cloud_volume + "' not found")
                sys.exit(2)
            snapshot_id = self.get_snapshot_id(subscription_id, credentials,
                vol, snapshot_name, verbose)
            if snapshot_id:
                print("Error - snapshot '" + snapshot_name + "' already exists")
                sys.exit(2)
            volumes.update([(cloud_volume, vol)])

        return volumes

    #
    # Create a snapshot of each cloud volume in the list
    # - assumes the cloud volume list has already been validated
    #
    def create_snapshot_internal(self, volumes, cloud_volumes, subscription_id,
        credentials, snapshot_name, verbose):
        anf_client = AzureNetAppFilesManagementClient(credentials,
            subscription_id)

        start_time = datetime.datetime.now()
        for cloud_volume in cloud_volumes:
            volume = volumes[cloud_volume]
            resource_group, netapp_account, capacity_pool = \
                self.parse_volume_id(volume)
            volume_name = volume.creation_token
            snapshot_body = Snapshot(location=volume.location,
                file_system_id=volume.file_system_id)
            # this is a blocking call, so no need to wait for a response
            anf_client.snapshots.create(snapshot_body, resource_group, 
                netapp_account, capacity_pool, volume_name, 
                snapshot_name).result()
        elapsed = datetime.datetime.now() - start_time

        print("Created snapshot '" + snapshot_name + "' in " + \
            str(elapsed.total_seconds()) + " seconds")
        return True

    #
    # Create a snapshot of each cloud volume in the list
    #
    def create_snapshot(self, cloud_volumes, snapshot_name, system_id,
        credentials, client_id, verbose):
        subscription_id = client_id

        # validate arguments
        if not subscription_id:
            print("Error - \"subscription_id\" unknown, specify in " + \
                "configuration file")
            sys.exit(2)
        if not snapshot_name:
            snapshot_name = create_snapshot_name()
        if not cloud_volumes:
            print("Error - no cloud volumes specified, specify with " + \
                "--cloud-volumes or in configuration file")
            sys.exit(2)
        if verbose:
            print("Preparing to create snapshot of: " + \
                ", ".join(cloud_volumes))
        volumes = self.validate_cloud_volumes(cloud_volumes, subscription_id,
            credentials, snapshot_name, verbose)

        self.create_snapshot_internal(volumes, cloud_volumes, subscription_id,
            credentials, snapshot_name, verbose)

    #
    # Create an application-consistent snapshot of a HANA database
    #
    def hana_backup(self, cloud_volumes, snapshot_name, system_id,
        userstore_key, auth, client_id, verbose):
        credentials = auth
        subscription_id = client_id

        # validate arguments
        if not subscription_id:
            print("Error - \"subscription_id\" unknown, specify in " + \
                "configuration file")
            sys.exit(2)
        if not snapshot_name:
            snapshot_name = create_snapshot_name()
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
        volumes = self.validate_cloud_volumes(cloud_volumes, subscription_id,
            credentials, snapshot_name, verbose)

        # open HANA backup
        open_backup_internal(snapshot_name, system_id, userstore_key, verbose)

        try:
            result = self.create_snapshot_internal(volumes, cloud_volumes,
                subscription_id, credentials, snapshot_name, verbose)
        except:
            result = False

        # close HANA backup
        close_backup_internal(snapshot_name, system_id, userstore_key, \
            result, verbose)

    #
    # Find a mount point on the host for a cloud volume
    #
    def get_mount_point(self, cloud_volume, subscription_id, credentials, 
        verbose):
        cloud_volume_candidate = run_command(["/bin/findmnt", cloud_volume, \
            "-no", "SOURCE"], False, return_result=True, suppress_error=True)
        if cloud_volume_candidate != HANA_NOT_RUNNING:
            return cloud_volume

        if not subscription_id:
            print("Error - \"subscription_id\" unknown, specify in " + \
                "configuration file")
            sys.exit(2)
        if not credentials:
            print("Error - KEY_FILE not found, specify path")
            sys.exit(2)

        volume = self.get_volume(cloud_volume, subscription_id, credentials, 
            verbose)
        if not volume:
            print("Volume '" + cloud_volume + "' not found")
            sys.exit(2)

        export = volume.creation_token
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
    # Copy the contents of a snapshot into the active filesystem
    # - the database must be stopped first
    #
    def restore(self, cloud_volume, snapshot, system_id, userstore_key, auth,
        client_id, verbose):
        credentials = auth
        subscription_id = client_id

        if is_hana_running(system_id, userstore_key, verbose):
            print("Error - database must be stopped before it can be restored")
            sys.exit(2)

        mount_point = self.get_mount_point(cloud_volume, subscription_id, 
            credentials, verbose)

        restore_internal(mount_point, snapshot, verbose)

    #
    # Provision a new cloud volume which is a clone of the snapshot of
    # another cloud volume
    #
    def clone(self, cloud_volume, snapshot, volume_name, export_path, cidr,
        auth, client_id, verbose):
        credentials = auth
        subscription_id = client_id

        if export_path:
            print("Error - setting the EXPORT_PATH not surrently supported")
            sys.exit(2)
        if cidr:
            print("Error - setting the CIDR not surrently supported")
            sys.exit(2)

        # validate arguments
        if not subscription_id:
            print("Error - \"subscription_id\" unknown, specify in " + \
                "configiuration file")
            sys.exit(2)
        if not volume_name:
            print("Error - VOLUME_NAME is a required argument")
            sys.exit(2)
        volume = self.get_volume(volume_name, subscription_id, credentials, 
            False)
        if volume:
            print("Error - volume '" + volume_name + "' already exists")
            sys.exit(2)
        if not cloud_volume:
            print("Error - CLOUD_VOLUME is a required argument")
            sys.exit(2)
        volume = self.get_volume(cloud_volume, subscription_id, credentials, 
            verbose)
        if not volume:
            print("Error - volume '" + cloud_volume + "' not found")
            sys.exit(2)
        if not snapshot:
            print("Error - SNAPSHOT is a required argument")
            sys.exit(2)
        snapshot_id = self.get_snapshot_id(subscription_id, credentials, 
            volume, snapshot, verbose)
        if not snapshot_id:
            print("Error - snapshot '" + snapshot + "' not found")
            sys.exit(2)

        anf_client = AzureNetAppFilesManagementClient(credentials,
            subscription_id)
        resource_group, netapp_account, capacity_pool = \
            self.parse_volume_id(volume)

        volume_body = Volume(
            location = volume.location,
            usage_threshold = volume.usage_threshold,
            snapshot_id = snapshot_id,
            creation_token = volume_name,
            service_level = volume.service_level,
            subnet_id = volume.subnet_id
        )
        start_time = datetime.datetime.now()
        try:
            anf_client.volumes.create_or_update(volume_body, 
                resource_group, netapp_account, capacity_pool, 
                volume_name)
            elapsed = datetime.datetime.now() - start_time
            print("Created clone '" + volume_name + "' in " + \
                str(elapsed.total_seconds()) + " seconds")
        except:
            print("Error - clone failed to initialize: '" + volume_name + "'")

    #
    # List the snapshots of a cloud volume
    #
    def list_snapshots(self, cloud_volume, system_id, auth, client_id,
        verbose):
        credentials = auth
        subscription_id = client_id

        # validate arguments
        if not subscription_id:
            print("Error - \"subscription_id\" unknown, specify in " + \
                "configiuration file")
            sys.exit(2)
        if not cloud_volume:
            print("Error - CLOUD_VOLUME is a required argument")
            sys.exit(2)
        volume = self.get_volume(cloud_volume, subscription_id, credentials, 
            verbose)
        if not volume:
            print("Error - volume '" + cloud_volume + "' not found")
            sys.exit(2)

        anf_client = AzureNetAppFilesManagementClient(credentials,
            subscription_id)

        resource_group, netapp_account, capacity_pool =  \
            self.parse_volume_id(volume)
        cloud_volume = volume.creation_token

        snapshots = anf_client.snapshots.list(resource_group, 
            netapp_account, capacity_pool, cloud_volume)

        # use a standard library to format the table
        row_format ="{:>30} {:>30}"
        print(row_format.format("Name", "Created"))
        for snapshot in snapshots:
            try:
                date = str(snapshot.created)
            except:
                # the python 2 version of the SDK doesn't return creation dates
                date = "None"
            print(row_format.format(snapshot.name.split("/")[3], date))

    #
    # Delete a snapshot 
    # - snapshots which are the bases of clones cannot be deleted
    #
    def delete_snapshot(self, cloud_volume, snapshot_name, all_previous, 
        system_id, auth, client_id, verbose):
        credentials = auth
        subscription_id = client_id

        # validate arguments
        if not subscription_id:
            print("Error - \"subscription_id\" unknown, specify in " + \
                "configiuration file")
            sys.exit(2)
        if not cloud_volume:
            print("Error - CLOUD_VOLUME is a required argument")
            sys.exit(2)
        if not snapshot_name:
            print("Error - SNAPSHOT is a required argument")
            sys.exit(2)
        volume = self.get_volume(cloud_volume, subscription_id, credentials,
             verbose)
        if not volume:
            print("Error - volume '" + cloud_volume + "' not found")
            sys.exit(2)

        anf_client = AzureNetAppFilesManagementClient(credentials,
            subscription_id)
        resource_group, netapp_account, capacity_pool =  \
            self.parse_volume_id(volume)
        try:
            snapshot = anf_client.snapshots.get(resource_group,
                netapp_account, capacity_pool, cloud_volume, snapshot_name)
        except:
            print("Error - snapshot '" + snapshot_name + "' not found")
            sys.exit(2)

        # get a list of all snapshot we might want to delete
        if all_previous:
            try:
                created = snapshot.created
                if verbose:
                    print("Delete all snapshots before " + str(created))
                snapshots = anf_client.snapshots.list(resource_group, 
                    netapp_account, capacity_pool, cloud_volume)
            except:
                # the python 2 version of the SDK doesn't return creation dates
                print("Error - no creation dates found on snapshots, " + \
                    "no snapshots deleted")
                sys.exit(2)
            for candidate in snapshots:
                date = candidate.created
                if date <= created:
                    if verbose:
                        print("Delete snapshot: " + candidate.name)
                    try:
                        anf_client.snapshots.delete(resource_group,
                            netapp_account, capacity_pool, cloud_volume, 
                            candidate.name.split("/")[3])
                        print("Snapshot '" + candidate.name + "' deleted")
                    except:
                        print("Error - '" + candidate.name + "' not deleted")
        else:
            if verbose:
                print("Delete snapshot: " + snapshot.name)
            try:
                anf_client.snapshots.delete(resource_group,
                    netapp_account, capacity_pool, cloud_volume, snapshot_name)
                print("Snapshot '" + snapshot_name + "' deleted")
            except:
                print("Error - '" + snapshot_name + "' not deleted")

if __name__ == "__main__":
    # create platform-specific object
    CVS = ANF()

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
        --snapshot SNAPSHOT [--all-previous] [--SID SID] [--key-file KEY_FILE] \
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
    client_id, system_id, userstore_key, cloud_volumes, network = \
        CVS.get_config(args.config_file, args.key_file, args.SID, 
        args.userstore_key, args.cloud_volumes, args.verbose)

    if args.hana_backup:
        CVS.hana_backup(cloud_volumes, args.backup_name, system_id, 
            userstore_key, auth, client_id, args.verbose)
    elif args.create_snapshot:
        CVS.create_snapshot(cloud_volumes, args.snapshot_name, system_id, 
            auth, client_id, args.verbose)
    elif args.open_backup:
        open_backup(args.EBID, system_id, userstore_key, args.verbose)
    elif args.close_backup:
        close_backup(args.EBID, system_id, userstore_key, args.verbose)
    elif args.restore:
        CVS.restore(args.cloud_volume, args.snapshot, system_id, userstore_key,
            auth, client_id, args.verbose)
    elif args.clone:
        CVS.clone(args.cloud_volume, args.snapshot, args.volume_name,
            args.export_path, args.CIDR, auth, client_id, args.verbose)
    elif args.list_snapshots:
        CVS.list_snapshots(args.cloud_volume, system_id, auth, client_id, 
            args.verbose)
    elif args.delete_snapshot:
        CVS.delete_snapshot(args.cloud_volume, args.snapshot, args.all_previous,
            system_id, auth, client_id, args.verbose)
    else:
        print("Error: specify --hana-backup, --create-snapshot, " + \
            "--open-backup, --close-backup, --restore, --clone, " + \
            "--list-snapshots, or --delete-snapshot")



