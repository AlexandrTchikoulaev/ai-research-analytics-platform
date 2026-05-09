import boto3

def clear_bucket(bucket_name, s3_client):
    print(f"A limpar bucket: {bucket_name}")

    paginator = s3_client.get_paginator('list_objects_v2')

    total_deleted = 0

    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]

            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )

            deleted_count = len(response.get('Deleted', []))
            total_deleted += deleted_count

            print(f"{deleted_count} objetos apagados nesta página")

    print(f"Total apagado no bucket {bucket_name}: {total_deleted}")


def clear_buckets():
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9002',  # MinIO
        aws_access_key_id='admin',
        aws_secret_access_key='admin123'
    )

    buckets = ['bronze', 'silver']

    for bucket in buckets:
        clear_bucket(bucket, s3)


if __name__ == "__main__":
    clear_buckets()