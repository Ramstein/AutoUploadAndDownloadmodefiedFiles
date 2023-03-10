import logging
import os
from asyncio.log import logger

import boto3
import botocore
from botocore.exceptions import ClientError
from botocore.exceptions import ClientError


class S3:

    def __init__(self, region='ap-south-1', bucket='', profile_name='default'):
        self.region = region
        self.bucket = bucket
        self.profile = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.s3_resource = self.session.resource('s3', region_name=region)
        self.s3_client = self.session.client('s3', region_name=region)
        self.s3_bucket = self.s3_resource.Bucket(self.bucket)

    def upload_to_s3(self, channel, filepath, public=False, dir_to_upload=''):
        # if not self.s3_resource:
        #     self.session = boto3.session.Session(profile_name=self.profile)
        #     self.s3_resource = boto3.resource('s3', region_name=self.region)
        if not self.s3_client:
            self.session = boto3.session.Session(profile_name=self.profile)
            self.s3_client = self.session.client('s3', region_name=self.region)
        if not self.s3_bucket:
            self.s3_bucket = self.s3_resource.Bucket(self.bucket)

        # data = open(filepath, "rb")
        # key = channel + '/' + str(filepath).split(channel)[-1]

        relative_path = os.path.relpath(filepath, dir_to_upload)
        s3_path = os.path.join(channel, relative_path).replace("\\", "/")
        if public:
            # print(f"Uploading file {filepath} to s3://{self.bucket}/{channel} with ACL='public-read'")
            # self.s3_bucket.put_object(Key=key, Body=data, ACL='public-read')
            self.s3_client.upload_file(filepath, Bucket=self.bucket, Key=s3_path, ExtraArgs={'ACL': 'public-read'})
        else:
            # print(f"Uploading file {filepath} to s3://{self.bucket}/{channel}")
            # self.s3_bucket.put_object(Key=key, Body=data)
            self.s3_client.upload_file(filepath, Bucket=self.bucket, Key=s3_path)

    def upload_dir_to_s3(self, bucket, s3_folder, dir_to_upload, region=''):
        s3_client = boto3.client('s3', region_name=region)
        print(f"Uploading {dir_to_upload} to s3://{self.bucket}/{s3_folder}")
        # enumerate local files recursively
        for root, dirs, files in os.walk(dir_to_upload):
            for filename in files:
                # construct the full local path
                local_path = os.path.join(root, filename)
                # construct the full Dropbox path
                relative_path = os.path.relpath(local_path, dir_to_upload)
                s3_path = os.path.join(s3_folder, relative_path).replace("\\", "/")
                try:
                    s3_client.head_object(Bucket=self.bucket, Key=s3_path)
                    print("Path found on S3! Deleting %s..." % s3_path)
                    try:
                        s3_client.delete_object(Bucket=self.bucket, Key=s3_path)
                        try:
                            # print("Uploading {} to s3://{}/{}".format(dir_to_upload, self.bucket, s3_path)
                            s3_client.upload_file(local_path, Bucket=self.bucket, Key=s3_path)
                        except ClientError as e:
                            logging.error(e)
                    except:
                        print("Unable to delete from s3 %s..." % s3_path)
                except:
                    try:
                        s3_client.upload_file(local_path, Bucket=self.bucket, Key=s3_path)
                    except ClientError as e:
                        logging.error(e)
        print("Upload completed successfully.")

    def download_from_s3(self, region='', bucket="", s3_filename='test.png', local_path=""):
        s3_client = boto3.client('s3', region_name=region)
        try:
            s3_client.download_file(self.bucket, Key=s3_filename, Filename=local_path)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info(f"The object s3://{self.bucket}/{s3_filename} in {region} does not exist.")
            else:
                raise

    def download_dir(self, s3_folder, local_path, bucket="", region=""):
        """
        params:
        - s3_folder: pattern to match in s3
        - local_path: local_path path to folder in which to place files
        - bucket: s3 bucket with target contents
        - client: initialized s3 client object
        """
        client = boto3.client('s3', region_name=region)
        keys = []
        dirs = []
        next_token = ''
        base_kwargs = {
            'Bucket': self.bucket,
            'Prefix': s3_folder,
        }
        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = client.list_objects_v2(**kwargs)
            contents = results.get('Contents')
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get('NextContinuationToken')
        for d in dirs:
            dest_pathname = os.path.join(local_path, d)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
        print("{} files found in {} directories. Downloading now...".format(len(keys), len(dirs)))
        for k in keys:
            dest_pathname = os.path.join(local_path, k)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            try:
                #             print("Downloading {}".format(dest_pathname))
                client.download_file(self.bucket, k, dest_pathname)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise
        print("{} files downloaded successfully.".format(len(keys)))

    # download('http://data.lip6.fr/cadene/pretrainedmodels/se_resnext50_32x4d-a260b3a4.pth')
    # upload_to_s3("pretrained", 'se_resnext50_32x4d-a260b3a4.pth')
    # download_dir(s3_folder='aptos-2015/test_images_768/', local_path='/home/ec2-user/SageMaker/data/aptos-2015/test_images_768/', bucket=self.bucket)
    # download_from_s3(s3_filename='pytorch-training-2020-12-29-09-38-13-247/source/sourcedir.tar.gz', local_path="/home/ec2-user/SageMaker/checkpoint/sourcedir.tar.gz")
