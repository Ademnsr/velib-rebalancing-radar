#Fonctions partagées entre les scripts d'extraction
import boto3


def upload_to_s3(local_path, s3_key, bucket_name):
    s3_client = boto3.client("s3")
    s3_client.upload_file(str(local_path), bucket_name, s3_key)