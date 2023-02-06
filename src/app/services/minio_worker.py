import io

from minio import Minio
from minio.error import S3Error


def create_client():
    print('Getting minio client.')
    try:
        client = Minio(
            "play.min.io",
            access_key="jIvqJRDwSrF9ovDo",
            secret_key="c2XIqaN5cO4TLpsQMCeTDOpm9xjv3sbb"
        )
        return client
    except S3Error as exc:
        print("error occurred.", exc)


def upload_data(data):
    print('Starting upload data to minio.')
    client = create_client()

    found = client.bucket_exists("stock-quotes")
    if not found:
        print("Creating bucket")
        client.make_bucket("stock-quotes")

    data = str(data)
    result = client.put_object(
        "stock-quotes", "my-object", io.BytesIO(bytes(data)), len(data),
        metadata={"My-Project": "one"},
    )


if __name__ == '__main__':
    upload_data('12234')
