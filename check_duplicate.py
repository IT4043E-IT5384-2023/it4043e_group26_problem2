import json
import os
from google.cloud import storage
folder_path = 'raw_data'

def upload_file_to_google_cloud_storage(bucket_name, file_name, local_csv_path):
    client = storage.Client.from_service_account_json('service-account\lucky-wall-393304-2a6a3df38253.json')
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(local_csv_path, content_type='application/json')

    print(f'File {local_csv_path} uploaded to {file_name} in {bucket_name} bucket.')
# Get the list of files in the folder
files = os.listdir(folder_path)
data = []
# Print the list of files
for file in files:
    with open(os.path.join(folder_path, file), 'r', encoding='utf-8') as f:
        data += json.load(f, strict=False)
print(len(data))

unique_values = set()
result = []

with open('output_notclean.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, indent=2)

for item in data:
    if 'tweets' in item:
        if item['all_tweets_id'][0] not in unique_values:
            unique_values.add(item['all_tweets_id'][0])
            for tweet in item['tweets']:
                result.append(tweet)
    elif item['original_tweet']['conversation_id_str'] not in unique_values:
        unique_values.add(item['original_tweet']['conversation_id_str'])
        result.append(item)
print(len(result))

#write result
with open('output.json', 'w', encoding='utf-8') as file:
    json.dump(result, file, indent=2)

upload_file_to_google_cloud_storage('it4043e-it5384', 'it4043e/it4043e_group26_problem2/output/output_notclean.json', 'output_notclean.json')
upload_file_to_google_cloud_storage('it4043e-it5384', 'it4043e/it4043e_group26_problem2/output/output.json', 'output.json')