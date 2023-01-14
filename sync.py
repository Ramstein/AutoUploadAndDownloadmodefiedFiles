import json
import os
import time
from datetime import datetime, timezone

import boto3
from tqdm import tqdm

from S3Handler import S3

root_path = r"C:\Users\ZEESHAN ALAM\Documents"
work_dirs = ['TLI_aaaaa', 'SC_baaaa']
upload_record_files = ['tli_upload_sync_record.json', 'sc_upload_sync_record.json']
download_record_files = ['tli_download_sync_record.json', 'sc_download_sync_record.json']

profile_name = 'workspace_sync_files_s3_full_access'
region = 'ap-south-1'
bucket = 'workspace-sync-files'
s3 = S3(region='ap-south-1', bucket=bucket, profile_name=profile_name)

download = True

while True:
    dt1 = datetime.now(timezone.utc)
    print('Initiating Sync at ', dt1)

    if download:
        download = False

        today = datetime.now(timezone.utc)

        s3 = boto3.client('s3', region_name=region)
        objects = s3.list_objects(Bucket=bucket)

        for work_dir, upload_record_file, download_record_file in zip(work_dirs, upload_record_files, download_record_files):
            work_dir_full_path = os.path.join(root_path, work_dir)
            upload_record_file_full_path = os.path.join(root_path, upload_record_file)
            download_record_file_full_path = os.path.join(root_path, download_record_file)
            with open(upload_record_file_full_path, 'r') as fp:
                upload_sync_mtimes = json.loads(str(fp))
            with open(download_record_file_full_path, 'r') as fp:
                download_sync_mtimes = json.loads(str(fp))
            print(f"Syncing files of {work_dir_full_path} with sync_record_file {download_record_file_full_path}")

            for dirname, subdirs, files in tqdm(os.walk(work_dir_full_path)):
                for fname in files:
                    full_path = os.path.join(dirname, fname)
                    f_mtime = os.stat(full_path).st_mtime

                    try:
                        if not sync_f_mtimes[full_path]:
                            s3.upload_to_s3(channel=work_dir, filepath=full_path, dir_to_upload=work_dir_full_path)
                            sync_f_mtimes[full_path] = f_mtime
                        else:
                            if f_mtime > sync_f_mtimes[full_path]:
                                s3.upload_to_s3(channel=work_dir, filepath=full_path, dir_to_upload=work_dir_full_path)
                                sync_f_mtimes[full_path] = f_mtime
                    except Exception as e:
                        print(e, full_path)

            with open(upload_record_file, 'w') as fp:
                json.dump(sync_f_mtimes, fp)

            for object in objects:
                if object['Contents']["LastModified"] == today:
                    print(object['Contents']["Key"])

            s3_client = boto3.client('s3', region_name=region)
            # try:
            #     s3_client.download_file(bucket, Key=s3_filename, Filename=local_path)
            # except ClientError as e:
            #     if e.response['Error']['Code'] == "404":
            #         logger.info(f"The object s3://{self.bucket}/{s3_filename} in {region} does not exist.")
            #     else:
            #         raise








    else:
        for work_dir, upload_record_file, download_record_file in zip(work_dirs, upload_record_files, download_record_files):
            work_dir_full_path = os.path.join(root_path, work_dir)
            upload_record_file_full_path = os.path.join(root_path, upload_record_file)
            with open(upload_record_file_full_path, 'r') as fp:
                sync_f_mtimes = json.loads(str(fp))
            print(f"Syncing files of {work_dir_full_path} with sync_record_file {upload_record_file_full_path}")

            for dirname, subdirs, files in tqdm(os.walk(work_dir_full_path)):
                for fname in files:
                    full_path = os.path.join(dirname, fname)
                    f_mtime = os.stat(full_path).st_mtime

                    try:
                        if not sync_f_mtimes[full_path]:
                            s3.upload_to_s3(channel=work_dir, filepath=full_path, dir_to_upload=work_dir_full_path)
                            sync_f_mtimes[full_path] = f_mtime
                        else:
                            if f_mtime > sync_f_mtimes[full_path]:
                                s3.upload_to_s3(channel=work_dir, filepath=full_path, dir_to_upload=work_dir_full_path)
                                sync_f_mtimes[full_path] = f_mtime
                    except Exception as e:
                        print(e, full_path)

            with open(upload_record_file, 'w') as fp:
                json.dump(sync_f_mtimes, fp)

    dt2 = datetime.now(timezone.utc)
    tdelta_sec = (dt2 - dt1).total_seconds()

    if tdelta_sec > 300:
        print('Initiating Next Sync at ', dt2)
        continue
    else:
        # waiting for 5 minutes
        print(f'Waiting for {300 - tdelta_sec}')
        time.sleep(300 - tdelta_sec)
