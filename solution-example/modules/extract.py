import requests
import subprocess
import os
import wget


def download_csv_requests(url, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with requests.Session() as session:
        response = session.get(url, stream=True)
        response.raise_for_status()
        with open(path, 'wb') as f: 
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


def download_csv_wget(url, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    wget.download(url, out=path)


def download_csv_curl(url, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    subprocess.run(['curl', '-L', '-o', path, url], check=True)


# Default method used by main
def download_csv(url, path):
    download_csv_requests(url, path)
