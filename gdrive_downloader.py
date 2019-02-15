import json
import os
import configparser
import random
import re
import io
import httplib2
import logging
import csv
import pathlib
import time

from datetime import datetime


from googleapiclient import errors
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

downloaded_file = 0
downloaded_error_file = 0
downloaded_revision = 0
downloaded_error_revision = 0
maximum_backoff = 8  # 32 #or 64 second


HOME_FOLDER = "/media/ossama/Data/TesiWorkStation/"
DESTINATION_FOLDER = "Download/"
DESTINATION_METADATA = "Metadata/"

config = configparser.ConfigParser()
config.read('config/config.cfg')
TOKENS = HOME_FOLDER + config.get('gdrive', 'tokenfile')
CLIENT_SECRETS = config.get('gdrive', 'configurationfile')
LOGFILE = HOME_FOLDER + 'app.log'

FIELDS_NAME = ['downloadTime', 'remotePath',
               'kind', 'id', 'name', 'mimeType', 'description', 'starred', 'trashed', 'explicitlyTrashed',
               'trashingUser', 'trashedTime', 'parents', 'properties', 'appProperties', 'spaces', 'version',
               'webContentLink', 'webViewLink', 'iconLink', 'hasThumbnail', 'thumbnailLink', 'thumbnailVersion',
               'viewedByMe', 'viewedByMeTime', 'createdTime', 'modifiedTime', 'modifiedByMeTime', 'modifiedByMe',
               'sharedWithMeTime', 'sharingUser', 'owners', 'teamDriveId', 'lastModifyingUser', 'shared', 'ownedByMe',
               'capabilities', 'viewersCanCopyContent', 'copyRequiresWriterPermission', 'writersCanShare',
               'permissions', 'permissionIds', 'hasAugmentedPermissions', 'folderColorRgb', 'originalFilename',
               'fullFileExtension', 'fileExtension', 'md5Checksum', 'size', 'quotaBytesUsed', 'headRevisionId',
               'contentHints', 'imageMediaMetadata', 'videoMediaMetadata', 'isAppAuthorized', 'exportLinks']

FIELDS_NAME_R = ['downloadTime', 'remotePath',
                 'kind', 'etag', 'id', 'selfLink', 'mimeType', 'modifiedDate', 'pinned', 'published', 'publishedLink',
                 'publishAuto', 'publishedOutsideDomain', 'downloadUrl', 'exportLinks', 'lastModifyingUserName',
                 'lastModifyingUser', 'originalFilename', 'md5Checksum', 'fileSize']

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/drive']
SCOPE = 'https://www.googleapis.com/auth/drive'
API_SERVICE_NAME = 'drive'
API_VERSION = 'v3'

# Helpful message to display in the browser if the CLIENT_SECRETS file
# is missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0
To make this sample run you will need to populate the config/gdrive_config.json file
found at:
   %s
with information from the APIs Console <https://code.google.com/apis/console>.
""" % os.path.join(os.path.dirname(__file__), CLIENT_SECRETS)

# Set up a Flow object to be used if we need to authenticate.
FLOW = flow_from_clientsecrets(CLIENT_SECRETS, scope=SCOPE,
                               message=MISSING_CLIENT_SECRETS_MESSAGE)


def create_dir(directory):
    if not os.path.exists(directory.encode('utf-8')):
        os.makedirs(directory.encode('utf-8'))


def init_csv_file():
    with open(CSV_FILE, "w") as f:
        writer_csv = csv.DictWriter(f, fieldnames=FIELDS_NAME)
        writer_csv.writeheader()
    f.close()
    with open(CSV_REVISION, "w") as f:
        writer_csv = csv.DictWriter(f, fieldnames=FIELDS_NAME_R)
        writer_csv.writeheader()
    f.close()


def write_row(item):
    try:
        with open(CSV_FILE, "a+") as f:
            writer_csv = csv.DictWriter(f, fieldnames=FIELDS_NAME)
            writer_csv.writerow(item)
        f.close()
    except Exception as error:
        logger.error('An error occurred in write row: {}'.format(error))


def write_row_revision(item):
    try:
        with open(CSV_REVISION, "a+") as f:
            writer_csv = csv.DictWriter(f, fieldnames=FIELDS_NAME_R)
            writer_csv.writerow(item)
        f.close()
    except Exception as error:
        logger.error('An error occurred in write row revision: {}'.format(error))


def is_file(item):
    return item['mimeType'] != 'application/vnd.google-apps.folder'


def is_folder(item):
    return item['mimeType'] == 'application/vnd.google-apps.folder'


def is_google_doc(drive_file):
    return True if re.match('^application/vnd\.google-apps\..+', drive_file['mimeType']) else False


def retrieve_revisions(file_id):
    try:
        revisions = drive_service_v2.revisions().list(fileId=file_id).execute()
        if len(revisions.get('items', [])) > 1:
            return revisions.get('items', [])
        return None
    except errors.HttpError:
        return None


def try_download_url(downloadUrl, n):
    if n <= maximum_backoff:
        wait = (2 ** n) + (random.randint(0, 1000) / 1000)  # backoff
        time.sleep(wait)
        try:
            resp, content = drive_service_v2._http.request(downloadUrl)
            if resp.status == 200:
                return resp, content
            elif resp.status == 429:
                logger.debug("Cannot download file/revision, i try again attempt: {}".format(n + 1))
                return try_download_url(downloadUrl, n + 1)
            else:
                logger.error("Response of request url, return status {}".format(resp.status))
                return None, None
        except httplib2.HttpLib2Error as error:
            logger.error('An error occurred in request download url: {}'.format(error))
            return None, None
    else:
        return None, None


def download_revision(drive_file, revision_id, dest_path, folder_metadata):
    global downloaded_revision, downloaded_error_revision
    revision = None
    try:
        revision = drive_service_v2.revisions().get(fileId=drive_file['id'], revisionId=revision_id).execute()
    except Exception as error:
        downloaded_error_revision += 1
        logger.error('An error occurred in get single revision: {}'.format(error))
    if revision is not None:
        name_revision = "(" + revision['modifiedDate'] + ")" + drive_file['name'].replace('/', '_')
        file_location = dest_path + name_revision
        logger.debug(file_location)

        download_url = None

        if is_google_doc(drive_file):
            if drive_file['mimeType'] == 'application/vnd.google-apps.document':
                download_url = revision['exportLinks']['application/vnd.oasis.opendocument.text']
            if drive_file['mimeType'] == 'application/vnd.google-apps.presentation':
                download_url = revision['exportLinks']['application/pdf']
            if drive_file['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                download_url = revision['exportLinks']['application/vnd.oasis.opendocument.spreadsheet'] # TO DO Cambia in ODS
            if drive_file['mimeType'] == 'application/vnd.google-apps.drawing':
                download_url = revision['exportLinks']['image/jpeg']
        else:
            download_url = revision['downloadUrl']

        if download_url:
            resp, content = try_download_url(download_url, 0)
            if resp is not None:
                revision['downloadTime'] = str(datetime.now())
                try:
                    target = open(file_location.encode('utf-8'), 'wb')
                except:
                    logger.error(
                        'Could not open file %s for writing. Please check permissions. {} file_location'.format(
                            file_location))
                target.write(content)
                logger.debug("Download revision")
                save_metadata(revision, name_revision, folder_metadata)
                write_row_revision(revision)
                downloaded_revision += 1
            else:
                logger.error("Resp is None for revision {}".format(name_revision))
                downloaded_error_revision += 1
        else:
            downloaded_error_revision += 1
            logger.error("Download Url undefined")


def save_metadata(drive_file, drive_name, meta_folder):
    create_dir(meta_folder)
    file_json = meta_folder + drive_name + '.json'
    with open(file_json.encode('utf-8'), 'w+') as metadata_file:
        json.dump(drive_file, metadata_file)
    metadata_file.close()


def try_download_file(item, dest_path, folder_metadata): # download file with drive v2
    global downloaded_file, downloaded_error_file
    drive_file = drive_service_v2.files().get(fileId=item['id']).execute()
    download_url = None
    if is_google_doc(drive_file):
        if drive_file['mimeType'] == 'application/vnd.google-apps.document':
            download_url = drive_file['exportLinks']['application/vnd.oasis.opendocument.text']
        if drive_file['mimeType'] == 'application/vnd.google-apps.presentation':
            download_url = drive_file['exportLinks']['application/pdf']
        if drive_file['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            download_url = drive_file['exportLinks']['application/vnd.oasis.opendocument.spreadsheet']
        if drive_file['mimeType'] == 'application/vnd.google-apps.drawing':
            download_url = drive_file['exportLinks']['image/jpeg']
    else:
        download_url = drive_file['downloadUrl']

    if download_url:
        file_location = dest_path + item['name'].replace('/', '_')
        resp, content = try_download_url(download_url, 0)
        if resp is not None:
            item['downloadTime'] = str(datetime.now())
            try:
                target = open(file_location.encode('utf-8'), 'wb')
                target.write(content)
            except:
                logger.error(
                    'Could not open file %s for writing. Please check permissions. {} file_location'.format(
                        file_location))
            logger.debug("Download File with second attempt")
            save_metadata(item, item['name'].replace('/', '_'), folder_metadata)
            write_row(item)
            downloaded_file += 1
            return True
    else:
        downloaded_error_file += 1
        logger.error("Download Url undefined")
        return False


def download_file(item, dest_path, revision_list, folder_metadata):
    global downloaded_file, downloaded_error_file
    request = None
    file_location = dest_path + item['name'].replace('/', '_')
    if revision_list is not None:
        del revision_list[len(revision_list) - 1]
        for revision in revision_list:
            download_revision(item, revision['id'], dest_path, folder_metadata)

    if is_google_doc(item):
        mime_type = None
        if item['mimeType'] == 'application/vnd.google-apps.document':
            mime_type = 'application/vnd.oasis.opendocument.text'
        if item['mimeType'] == 'application/vnd.google-apps.presentation':
            mime_type = 'application/pdf'
        if item['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        if item['mimeType'] == 'application/vnd.google-apps.drawing':
            mime_type = 'image/jpeg'

        if mime_type is not None:
            request = drive_service_v3.files().export_media(fileId=item['id'], mimeType=mime_type)
    else:
        request = drive_service_v3.files().get_media(fileId=item['id'])

    if request:
        try:
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                logger.debug("Download %d%%." % int(status.progress() * 100))
            target = open(file_location.encode('utf-8'), 'wb')
            target.write(fh.getvalue())
            item['downloadTime'] = str(datetime.now())
            save_metadata(item, item['name'].replace('/', '_'), folder_metadata)
            write_row(item)
            downloaded_file += 1
        except Exception as error:
            if not try_download_file(item, dest_path, folder_metadata):
                downloaded_error_file += 1
                logger.error('An error occurred in download file {}: {}'.format(item['name'], error))


def get_files(drive_file, dest_path, remote_path, folder_metadata):

    drive_file['remotePath'] = str(pathlib.Path(remote_path + drive_file['name'].replace('/','_')))
    logger.debug(drive_file['remotePath'])
    revision_list = retrieve_revisions(drive_file['id'])

    download_file(drive_file, dest_path, revision_list, folder_metadata)


def get_content(folder, base_path='./'):
    result = []
    page_token = None
    next_page = True

    while next_page:
        try:
            param = dict()
            param['q'] = "'" + folder['id'] + "'" + " in parents"
            param['fields'] = '*'
            if page_token:
                param['pageToken'] = page_token
            folder_contents = drive_service_v3.files().list(**param).execute()
            result.extend(folder_contents['files'])
            page_token = folder_contents.get('nextPageToken')
            if not page_token:
                next_page = False
        except errors.Error as error:
            logger.error('An error occurred in get content of a folder: {}'.format(error))
            return

        dest_path = base_path + folder['name'].replace('/', '_') + '/'
        create_dir(dest_path)

        p = pathlib.Path(WORKSTATION_FOLDER)
        i = len(str(p).split("/"))
        f_p = pathlib.Path(dest_path)
        remote = str(pathlib.Path(*f_p.parts[i+1:])) + '/'
        folder_metadata = WORKSTATION_FOLDER + DESTINATION_METADATA + remote

        create_dir(folder_metadata)

        for item in filter(is_file, result):
            get_files(item, dest_path, remote, folder_metadata)
        for item in filter(is_folder, result):
            get_content(item, dest_path)


def get_others_content(query, base_path='./'):
    result = []
    page_token = None
    next_page = True
    dest_path = None
    while next_page:
        try:
            param = dict()
            param['q'] = query
            param['fields'] = '*'
            if page_token:
                param['pageToken'] = page_token
            folder_contents = drive_service_v3.files().list(**param).execute()
            result.extend(folder_contents['files'])
            page_token = folder_contents.get('nextPageToken')
            if not page_token:
                next_page = False
        except errors.Error as error:
            logger.error('An error occurred in get content of a folder: {}'.format(error))
            return

        if query == 'trashed':
            dest_path = base_path + 'Trash/'
        elif query == 'sharedWithMe':
            dest_path = base_path + 'Shared With Me/'
        logger.debug(dest_path)
        create_dir(dest_path)

        p = pathlib.Path(WORKSTATION_FOLDER)
        i = len(str(p).split("/"))
        f_p = pathlib.Path(dest_path)
        remote = str(pathlib.Path(*f_p.parts[i+1:])) + '/'
        folder_metadata = WORKSTATION_FOLDER + DESTINATION_METADATA + remote

        create_dir(folder_metadata)

        for item in filter(is_file, result):
            get_files(item, dest_path, remote, folder_metadata)
        for item in filter(is_folder, result):
            get_content(item, dest_path)


def get_user_info():
    try:
        about = drive_service_v3.about().get(fields="user").execute()
        return about
    except errors.HttpError as error:
        logger.error('An error occurred: {}'.format(error))
        return None


def main():
    start_time = datetime.now()
    logger.info('Starting at: {}'.format(start_time))

    global drive_service_v3, drive_service_v2, credentials, CSV_FILE, CSV_REVISION, WORKSTATION_FOLDER

    storage = Storage(TOKENS)
    credentials = None
    if credentials is None or credentials.invalid:
        credentials = run_flow(FLOW, storage, None)

    drive_service_v3 = build(API_SERVICE_NAME, 'v3', credentials=credentials)
    drive_service_v2 = build(API_SERVICE_NAME, 'v2', credentials=credentials)

    user_info = get_user_info()

    if user_info is not None:
        username = user_info['user']['emailAddress']
    else:
        username = '???'

    WORKSTATION_FOLDER = HOME_FOLDER + username + '/'
    create_dir(WORKSTATION_FOLDER + 'csv/')
    create_dir(WORKSTATION_FOLDER + DESTINATION_FOLDER)
    create_dir(WORKSTATION_FOLDER + DESTINATION_METADATA)

    CSV_FILE = WORKSTATION_FOLDER + config.get('gdrive', 'csvfile')
    CSV_REVISION = WORKSTATION_FOLDER + config.get('gdrive', 'csvrevision')

    init_csv_file()

    # get My Drive
    start_folder = drive_service_v3.files().get(fileId='root', fields='*').execute()
    #print("Start Folder", start_folder)
    # get_content(start_folder, WORKSTATION_FOLDER + DESTINATION_FOLDER)

    # get Files Trashed
    # get_others_content('trashed', WORKSTATION_FOLDER + DESTINATION_FOLDER)

    # get Files Shared
    get_others_content('sharedWithMe', WORKSTATION_FOLDER + DESTINATION_FOLDER)

    end_time = datetime.now()
    logger.info('Duration: {}'.format(end_time - start_time))
    logger.info("Downloaded File: {} - Error File: {} - Downloaded Revision: {} - Error Revision: {}".format(downloaded_file,
                                                                                                       downloaded_error_file,
                                                                                                       downloaded_revision,
                                                                                                       downloaded_error_revision))

    os.system('shutdown now')


def config_logger(logger):
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(filename=LOGFILE, mode='w')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s:%(funcName)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    config_logger(logger)
    main()
