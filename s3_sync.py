import os
import time
from datetime import datetime, timezone

root_path = r"C:\Users\ZEESHAN ALAM\Documents"
work_dirs = ['TLI', 'SC']
upload_record_files = ['tli_upload_sync_record.json', 'sc_upload_sync_record.json']
download_record_files = ['tli_download_sync_record.json', 'sc_download_sync_record.json']

profile_name = 'workspace_sync_files_s3_full_access'
region = 'ap-south-1'
bucket = 'workspace-sync-files'
delete = False

while True:
    dt1 = datetime.now(timezone.utc)
    print('Initiating Sync at', dt1)

    for work_dir, upload_record_file, download_record_file in zip(work_dirs, upload_record_files, download_record_files):
        work_dir_full_path = os.path.join(root_path, work_dir)
        print(work_dir_full_path)
        os.chdir(work_dir_full_path)

        if delete:
            # sync_command = f"aws s3 sync . 's3://{bucket}/{work_dir}' --region {region} --profile {profile_name} --delete"
            sync_command = f"aws s3 sync . s3://{bucket}/{work_dir} --region {region} --delete"
        else:
            sync_command = f"aws s3 sync . s3://{bucket}/{work_dir} --region {region}"
        print(sync_command)
        os.system(sync_command)

    dt2 = datetime.now(timezone.utc)
    tdelta_sec = (dt2 - dt1).total_seconds()

    if tdelta_sec > 300:
        print('Initiating Next Sync at', dt2)
    else:
        # waiting for 5 minutes
        print(f'Waiting for {300 - tdelta_sec}')
        time.sleep(300 - tdelta_sec)
