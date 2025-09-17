#!/usr/bin/env python3.13
import csv
import py7zr
import zipfile
from os import listdir, path, remove, makedirs
import requests
import re
import json
import tempfile
import sys
import hashlib

# сертификаты соответствия;
certificates_page = "https://fsa.gov.ru/opendata/7736638268-rss/"
# декларации соответствия.
declaration_page = "https://fsa.gov.ru/opendata/7736638268-rds/"

# add indent to make a JSON humanreadable but more storage-consuming
json_indent = True
# timeout for http request
timeout = 5

csv.field_size_limit(sys.maxsize)

class DownloadManager:
    def __init__(self, algorithm="md5", chuck_size=131072):
        self.algorithm = algorithm
        self.chuck_size = chuck_size


    def _calculate_hash(self, data: bytes) -> str:
        hasher = hashlib.new(self.algorithm)
        hasher.update(data)
        return hasher.hexdigest()

    def _calculate_file_hash(self, filepath: str) -> str|None:
        if not path.exists(filepath):
            return None
        hasher = hashlib.new(self.algorithm)
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(self.chuck_size), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def download_with_verification(self, url: str, filepath: str,
                                   force_download: bool = False):

        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        # Calculate hashes
        content_hash = self._calculate_hash(response.content)
        existing_hash = self._calculate_file_hash(filepath)

        result = {
            'url': url,
            'filepath': filepath,
            'content_hash': content_hash,
            'existing_hash': existing_hash,
            'file_existed': existing_hash is not None,
            'hashes_match': content_hash == existing_hash if existing_hash else False,
            'downloaded': False,
            'file_size': len(response.content)
        }

        should_save = force_download or not result['hashes_match']

        if should_save:
            makedirs(path.dirname(filepath) or '.', exist_ok=True)

            # Save file
            with open(filepath, 'wb') as f:
                f.write(response.content)

            result['downloaded'] = True

            # Verify saved file
            saved_hash = self._calculate_file_hash(filepath)
            result['save_verified'] = saved_hash == content_hash

        return result


def create_folder_structure():
    folders = ['./certificate/original',
               './declaration/original']
    for folder in folders:
        try:
            makedirs(folder, exist_ok=True)
        except FileExistsError:
            pass

def unzip_and_get_csv(file):
    with tempfile.TemporaryDirectory() as temp_dir:
        with py7zr.SevenZipFile(file) as z:
            z.extractall(path=temp_dir)

        files = listdir(temp_dir)

        for file in files:
            if file.endswith(".csv"):
                with open(path.join(temp_dir, file), 'r') as csvfile:
                    file_text = csvfile.readlines()

    return file_text


def parse_csv(file):
    file_text = unzip_and_get_csv(file)
    csv_data = csv.DictReader(file_text, delimiter=';')
    return csv_data


def get_files_from_fsa(url):
    response = requests.get(url)
    pattern = "<td>8</td>\n\\s+.+\n\\s+<td><a href=\"(.+)\" target"

    if response.status_code != 200:
        raise Exception(f"failed to get info from {url}. response code: {response.status_code}")

    link_to_file = re.findall(pattern, response.text)
    try:
        return link_to_file[0]
    except IndexError:
        raise Exception(f"link to file on {url} was not found.")


def clear_temp_directory(temp_dir):
    """Очистить все файлы во временной директории"""
    for file in listdir(temp_dir):
        file_path = path.join(temp_dir, file)
        try:
            remove(file_path)
        except OSError:
            pass  # Игнорировать ошибки, если файл уже удален


def process_data(data_type, url):
    print(f"Begin processing {data_type}...")
    manager = DownloadManager()
    create_folder_structure()
    file_link = get_files_from_fsa(url)
    filename = file_link.split('/')[-1]
    print(f"Downloading {filename}...")
    file = manager.download_with_verification(file_link, f"./{data_type}/original/{filename}")
    print(f"Downloaded {file}")
    if file['hashes_match']:
        print(f"The file {filename} has matching hashes with local file. "
              "No data needs to be processed.")
        return

    arch_date = re.search("^data-(.+)-structure", filename).groups()[0]
    year = arch_date[:4]

    makedirs(f"./{data_type}/{year}", exist_ok=True)

    file_count = 1
    zip_count = 1
    csv_data = parse_csv(f"./{data_type}/original/{file_link.split('/')[-1]}")

    # Create a context manager for a temporary dict. The context manager handles SIGINT SIGTERM and deletes temp dir
    print(f"Begin processing csv data...")
    with tempfile.TemporaryDirectory() as temp_dir:
        for row in csv_data:
            json_file_name = f"{data_type}-{arch_date}-{str(file_count):0>8}.json"
            with open(f'{temp_dir}/{json_file_name}', 'w', encoding="utf-8") as f:
                json.dump(dict(row), f,ensure_ascii=False, indent=2)

            if file_count % 1000 == 0:
                zip_file_name = f"./{data_type}/{year}/{data_type}-{arch_date}-{zip_count:0>3}.zip"
                print(f"\rProcessed rows: {file_count}", end='', flush=True)
                # print(f"saving to {zip_file_name}")
                with zipfile.ZipFile(zip_file_name, 'w') as z:
                    for file in listdir(temp_dir):
                        z.write(f"{temp_dir}/{file}", file)
                clear_temp_directory(temp_dir)
                zip_count += 1
            file_count += 1

        remaining_files = listdir(temp_dir)
        if remaining_files:
            print(f"\rProcessed rows: {file_count}", flush=True)
            with zipfile.ZipFile(zip_file_name, 'w') as z:
                for file in remaining_files:
                    z.write(f"{temp_dir}/{file}", file)


def get_certificates():
    process_data("certificate", certificates_page)


def get_declarations():
    process_data("declaration", declaration_page)


def main():
    get_certificates()
    get_declarations()


if __name__ == "__main__":
    main()
