import io

from minio import Minio
from minio.error import S3Error


def create_client():
    print('Getting minio client.')
    try:
        client = Minio(
            "localhost:9000",
            access_key="jIvqJRDwSrF9ovDo",
            secret_key="c2XIqaN5cO4TLpsQMCeTDOpm9xjv3sbb",
            secure=False
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

    print('Putting data')

    # data = str(data)
    # result = client.put_object(
    #     "stock-quotes", "stock-quotes-object", io.BytesIO(bytes(data, 'utf-8')), len(data),
    #     metadata={"Quotes_project": "one"},
    # )

    # result = client.put_object(
    #     "stock-quotes", "stock-quotes-object",
    # )
    return "Data was successfully uploaded"


def get_data():
    print('Starting get data from minio.')
    client = create_client()


if __name__ == '__main__':
    pass
