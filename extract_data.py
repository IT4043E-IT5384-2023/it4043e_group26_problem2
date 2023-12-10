import json
import pandas as pd
from functools import reduce
from google.cloud import storage

def upload_file_to_google_cloud_storage(bucket_name, file_name, local_csv_path):
    client = storage.Client.from_service_account_json('service-account\key.json')
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(local_csv_path, content_type='application/octet-stream')

    print(f'File {local_csv_path} uploaded to {file_name} in {bucket_name} bucket.')

with open("extract-field\tweet.txt", "r") as post:
    post_tag = [data.strip().split(" ") for data in post]
    post_data = {key : [] for key in [sub[-1] for sub in post_tag] }

with open("extract-field\user.txt", "r") as user:
    user_tag = [data.strip().split(" ") for data in user]
    user_data = {key : [] for key in [sub[-1] for sub in user_tag] }

file = open("output.json", 'r', encoding="utf-8")
data = json.load(file)
unique_user = set()

for post in data:
    #check language: if english -> continue
    if post["original_tweet"]["lang"] != "en":
        continue
    
    #check user are in set or not
    if post["author"]["original_user"]["screen_name"] not in unique_user:
        unique_user.add(post["author"]["original_user"]["screen_name"])

        #check post has "verified_type": dont have-> None
        if "verified_type" not in post['author']['original_user']:
            user_data["verified_type"].append(None)
        else:
            user_data["verified_type"].append(post['author']['original_user']['verified_type'])

        for utag in user_tag[1:]:
            user_data[utag[-1]].append(reduce(lambda d, k: d[k], utag, post))
    
    #get post data:
    #first get all tag:
    hashtagtext = ""
    for idx, hashtag in enumerate(post['hashtags']):
        if idx < len(post['hashtags']) - 1:
            hashtagtext += hashtag["text"] + ", "
        else:
            hashtagtext += hashtag["text"]
    post_data['hashtags'].append(hashtagtext)
    for ptag in post_tag[1:]:
        post_data[ptag[-1]].append(reduce(lambda d, k: d[k], ptag, post))

# detect bot
df_user = pd.DataFrame(user_data)
df_post = pd.DataFrame(post_data)

df_user['verified_type'] = df_user['verified_type'].fillna(value='none')
df_post['created_at'] = pd.to_datetime(df_post['created_at'], format='%a %b %d %H:%M:%S +0000 %Y')
df_post['created_at'] = (pd.to_datetime(df_post['created_at']).view('int64') // 10**9).astype('int64')
df_user['created_at'] = (pd.to_datetime(df_user['created_at']).view('int64') // 10**9).astype('int64')
# convert to parquet file
df_user.to_parquet('user_data.parquet', index=False)
df_post.to_parquet('post_data.parquet', index=False)

upload_file_to_google_cloud_storage('it4043e-it5384', 'it4043e/it4043e_group26_problem2/extract/user_data.parquet', 'user_data.parquet')
upload_file_to_google_cloud_storage('it4043e-it5384', 'it4043e/it4043e_group26_problem2/extract/post_data.parquet', 'post_data.parquet')