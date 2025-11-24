import os
import shutil
from tqdm import tqdm

# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
INPUT_FOLDER = r"D:\geosence\gdelt_unzipped"  # Folder containing extracted CSVs
OUTPUT_FOLDER = r"D:\geosence\gdelt_arranged"  # Final arranged folder
# ----------------------------------------------------

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Get all CSVs
csv_files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith(".csv")]

print(f"Found {len(csv_files)} CSV files\n")

# Progress bar
for file_name in tqdm(csv_files, desc="Arranging files by year"):

    # Example: 20230119.export.CSV → year = 2023
    try:
        year = file_name[0:4]  # first 4 characters
        if not year.isdigit():
            print(f"\n Skipping (invalid year): {file_name}")
            continue
    except:
        print(f"\nSkipping (bad filename): {file_name}")
        continue

    # Create year folder
    year_folder = os.path.join(OUTPUT_FOLDER, year)
    os.makedirs(year_folder, exist_ok=True)

    src = os.path.join(INPUT_FOLDER, file_name)
    dst = os.path.join(year_folder, file_name)

    # Skip if already moved
    if os.path.exists(dst):
        continue

    # Move file
    try:
        shutil.move(src, dst)
    except Exception as e:
        print(f"\n Error moving {file_name}: {e}")

print("\n All files arranged successfully!")
