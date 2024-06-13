import os
import hashlib
import requests
import csv
from google.cloud import storage

PREFIX = 'https://raw.githubusercontent.com/cd-public/books/main/'
BUCKET_NAME = 'bestest-bucket'
DIR_PATH = 'raw.githubusercontent.com/cd-public/books/main/'

def get_file_info(url):
    try:
        response = requests.head(url)
        file_size = int(response.headers.get('content-length', 0))
        if file_size == 0:
            return None, None
        md5_hash = hashlib.md5()
        with requests.get(url, stream=True) as r:
            for chunk in r.iter_content(chunk_size=8192):
                md5_hash.update(chunk)
        md5_checksum = md5_hash.hexdigest()
        return file_size, md5_checksum
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return None, None

def list_txt_files(bucket_name, dir_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=dir_path)
    txt_files = [blob.name for blob in blobs if blob.name.endswith('.txt')]
    return txt_files

def create_tsv(txt_files, output_tsv):
    with open(output_tsv, 'w', newline='') as outfile:
        writer = csv.writer(outfile, delimiter='\t')
        writer.writerow(['url', 'file_size', 'md5_checksum'])
        for file in txt_files:
            url = PREFIX + file[len(DIR_PATH):]
            file_size, md5_checksum = get_file_info(url)
            if file_size and md5_checksum:
                writer.writerow([url, file_size, md5_checksum])
    print(f"TSV file created at: {output_tsv}")

if __name__ == "__main__":
    txt_files = list_txt_files(BUCKET_NAME, DIR_PATH)
    output_tsv = 'books.tsv'
    create_tsv(txt_files, output_tsv)
