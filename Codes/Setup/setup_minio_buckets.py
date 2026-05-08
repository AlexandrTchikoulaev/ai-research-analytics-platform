import boto3

def main():
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9002',
        aws_access_key_id='admin',
        aws_secret_access_key='admin123'
    )

    buckets = ['bronze', 'silver', 'bronze-unstructured']

    for b in buckets:
        try:
            s3.head_bucket(Bucket=b)
            print(f"Bucket já existe: {b}")
        except:
            s3.create_bucket(Bucket=b)
            print(f"Bucket criado: {b}")


if __name__ == "__main__":
    main()