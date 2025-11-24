import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

url = 'http://data.gdeltproject.org/events/index.html'
year_start = 2013
year_end = 2025
max_threads = 10

download_dir = os.path.join(os.getcwd(), 'gdelt_event_files')
os.makedirs(download_dir, exist_ok=True)

def download_file(file_url, local_filename, href):
    try:
        with requests.get(file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return f"Saved: {href}"
    except Exception as e:
        return f"Failed {href}: {e}"

print("Fetching GDELT index page...")

try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
except Exception as e:
    print(f"Error fetching index page: {e}")
    exit()

soup = BeautifulSoup(response.text, 'html.parser')
file_pattern = re.compile(r'(\d{8})\.export\.CSV\.zip')

tasks = []

for link in soup.find_all('a', href=True):
    href = link['href']
    match = file_pattern.match(href)
    if match:
        file_date = match.group(1)
        file_year = int(file_date[:4])

        if year_start <= file_year <= year_end:
            local_filename = os.path.join(download_dir, href)
            file_url = f"http://data.gdeltproject.org/events/{href}"

            if os.path.exists(local_filename):
                print(f"{href} already exists, skipping.")
                continue

            tasks.append((file_url, local_filename, href))

print(f"Total files to download: {len(tasks)}")
print("Starting parallel downloads...\n")

with ThreadPoolExecutor(max_workers=max_threads) as executor:
    future_to_file = {
        executor.submit(download_file, file_url, local_filename, href): href
        for file_url, local_filename, href in tasks
    }

    for future in as_completed(future_to_file):
        print(future.result())

print("\nAll downloads complete!")
print(f"Files saved in: {download_dir}")
