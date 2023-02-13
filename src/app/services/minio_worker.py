# from minio import Minio
# from minio.error import S3Error
# # from src.config import MINIO_ENDPOINT, MINIO_BUCKET_NAME, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
#
#
# def create_client():
#     print('Getting minio client.')
#     try:
#         client = Minio(
#             MINIO_ENDPOINT,
#             access_key=MINIO_ACCESS_KEY,
#             secret_key=MINIO_SECRET_KEY,
#             secure=False
#         )
#         return client
#     except S3Error as exc:
#         print("error occurred.", exc)
#
#
# def upload_data(data):
#     print('Starting upload data to minio.')
#     client = create_client()
#
#     found = client.bucket_exists(MINIO_BUCKET_NAME)
#     if not found:
#         print("Creating bucket")
#         client.make_bucket(MINIO_BUCKET_NAME)
#
#     pass
#
#     return "Data was successfully uploaded"
#
#
# def get_data():
#     print('Starting get data from minio.')
#     client = create_client()
#
#
# if __name__ == '__main__':
#     pass
