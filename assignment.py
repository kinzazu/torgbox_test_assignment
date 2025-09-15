import csv
import py7zr
import zipfile
from os import mkdir, listdir, path, remove
import requests
import re
import json
import tempfile
import sys

# сертификаты соответствия;
certificates_page = "https://fsa.gov.ru/opendata/7736638268-rss/"
# декларации соответствия.
declaration_page = "https://fsa.gov.ru/opendata/7736638268-rds/"

# pretify data
json_indent = True
# timeout for http request
timeout = 5

csv.field_size_limit(sys.maxsize)



def create_folder_structure():
    folders = ['./certificate',
               './certificate/original',
               './declaration',
               './declaration/original']
    for folder in folders:
        try:
            mkdir(folder)
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


def check_hashsum():
    pass


def get_files_from_url(url):
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


def save_data(type, url):

    create_folder_structure()
    file_link = get_files_from_url(certificates_page)
    file = requests.get(file_link)
    if not file.ok:
        raise Exception(f"failed to get info from {file_link}")
    filename = file_link.split('/')[-1]
    arch_date = re.search("^data-(.+)-structure", filename).groups()[0]
    year = arch_date[:4]
    if filename in listdir(f"./{type}"):
        print('file already exists')
        exit(1)

    try:
        mkdir(f"./{type}/{year}")
    except FileExistsError:
        pass

    with open(f"./{type}/original/{file_link.split('/')[-1]}", "wb") as f:
        f.write(file.content)

    file_count = 1
    zip_count = 1


    csv_data = parse_csv(f"./{type}/original/{file_link.split('/')[-1]}")
    file_dict = {}
    with tempfile.TemporaryDirectory() as temp_dir:
        for row in csv_data:
            file_name = f"{type}-{arch_date}-{str(file_count):0>8}.json"
            with open(f'{temp_dir}/{file_name}', 'w') as f:
                json.dump(dict(row), f, indent=2)
            file_dict.update({file_name: json.dumps(dict(row))})

            if file_count % 1000 == 0:
                with zipfile.ZipFile(f"./{type}/{year}/{type}-{arch_date}-{zip_count:0>3}.zip", 'w') as z:
                    for file in listdir(temp_dir):
                        z.write(f"{temp_dir}/{file}", file)
                clear_temp_directory(temp_dir)
                zip_count += 1
            file_count += 1

        remaining_files = listdir(temp_dir)
        if remaining_files:
            with zipfile.ZipFile(f"./{type}/{year}/{type}-{arch_date}-{zip_count:0>3}.zip", 'w') as z:
                for file in remaining_files:
                    z.write(f"{temp_dir}/{file}", file)


def certicifates():
    save_data("certificate", certificates_page)


def declaration():
    save_data("declaration", declaration_page)

def main():
    certicifates()
    declaration()



if __name__ == "__main__":
    main()