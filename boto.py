import boto3
import pandas as pd

bucket = "appdanger" 
key = "03/ad37090df307326f6832abdc1062ab"
file = "ad37090df307326f6832abdc1062ab"

s3 = boto3.client("s3", 
        region_name='us-east-1', 
        aws_access_key_id="xxx", 
        aws_secret_access_key="xxx"
      )

# #list buckets on s3
# bucket_response = s3.list_buckets()
# buckets = bucket_response["Buckets"]
# print(buckets)

# # Create and delete buckets
# bucket = s3.create_bucket(Bucket=bucket)
# response = s3.delete_bucket(Bucket=bucket)

# List objects present in a bucket
# response = s3.list_objects(Bucket=bucket, MaxKeys=10)
# print(response)

# # Uploading and downloading files
# s3.upload_file(Filename=file, Bucket=bucket, Key=key)
# s3.download_file(Filename=file, Bucket=bucket, Key=key)

# # Get object's metadata (last modification time, size in bytes etc.)
# response = s3.head_object(Bucket=bucket, Key=key)

# # Delete object
# s3.delete_object(Bucket=bucket, Key=key)

#Loading multiple files into a single data frame
df_list = []
response = s3.list_objects(Bucket=bucket)
request_files = response["Contents"]
# print("Showing all objects")
# print(request_files)
# print("_______________________________________")
for _file in request_files:
    print("object: ")
    
    obj = s3.get_object(Bucket=bucket, Key=_file["Key"])
    obj_df = obj["Body"].read()
    print(obj_df)
    print("**************************************")
    df_list.append(obj_df)
df = pd.concat(df_list)
print(df)

# # Make existing object publicly available
# s3.put_object_acl(Bucket=bucket, 
#                   Key=key, 
#                   ACL="public-read")

# # Make an object public available on upload
# s3.upload_file(Filename=file, 
#                Bucket=bucket, 
#                Key=key, 
#                ExtraArgs={"ACL": "public"})