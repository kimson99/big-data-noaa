import requests
import os
import gzip

def download_and_group(start_year, end_year):
  base_url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/"
  save_dir = "../../../../resources/data"

  if not os.path.exists(save_dir):
    os.makedirs(save_dir)

  # Years to group together (e.g., before 1950)
  GROUP_THRESHOLD = 1950

  current_decade_file = None
  decade_buffer = []

  for year in range(start_year, end_year + 1):
    file_name = f"{year}.csv.gz"
    url = f"{base_url}{file_name}"

    print(f"Fetching {year}...", end="\r")

    try:
      response = requests.get(url, stream=True)
      if response.status_code == 200:
        # Read the compressed content
        content = response.content

        # DECISION: Group small years, keep recent years separate
        if year < GROUP_THRESHOLD:
          # Calculate decade (e.g., 1805 -> 1800)
          decade = (year // 10) * 10
          output_name = f"{decade}_{decade+9}.csv.gz"
          output_path = os.path.join(save_dir, output_name)

          # Append to the decade file
          # Mode 'ab' = Append Binary
          with open(output_path, 'ab') as f:
            f.write(content)

        else:
          # Save large years individually
          output_path = os.path.join(save_dir, file_name)
          with open(output_path, 'wb') as f:
            f.write(content)
      else:
        print(f"\nSkipping {year} (Not Found)")

    except Exception as e:
      print(f"\nError on {year}: {e}")

  print("\nDone! Small years are grouped by decade.")

if __name__ == "__main__":
  download_and_group(1959, 2025)
