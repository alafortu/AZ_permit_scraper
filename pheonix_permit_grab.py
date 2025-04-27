import requests
import json
import time
import csv
from datetime import datetime, timedelta

# --- PhoenixPermitScraper Class Definition ---
class PhoenixPermitScraper:
    """
    A class to scrape solar permit data from the City of Phoenix PDD API.
    """
    BASE_URL = "https://apps-secure.phoenix.gov/PDD/Search/Permits/_GetPermitData"
    DEFAULT_HEADERS = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest'
        # Add other headers like User-Agent if necessary based on testing
        # 'User-Agent': 'Mozilla/5.0 ...'
    }

    def __init__(self, page_size=50):
        """
        Initializes the scraper.

        Args:
            page_size (int): Number of results to request per API call.
        """
        self.page_size = page_size
        self.session = requests.Session() # Use a session for potential connection pooling
        self.session.headers.update(self.DEFAULT_HEADERS)

    def _make_request(self, payload):
        """
        Sends the POST request to the API endpoint.

        Args:
            payload (dict): The data payload for the POST request.

        Returns:
            dict: The parsed JSON response data, or None if the request fails.
        """
        try:
            response = self.session.post(self.BASE_URL, data=payload)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error during request: {e}")
        except json.JSONDecodeError:
            print(f"Error decoding JSON response. Status: {response.status_code}, Text: {response.text[:200]}...")
        except Exception as e:
            print(f"An unexpected error occurred during request: {e}")
        return None

    def _parse_date(self, date_string):
        """
        Parses the Microsoft JSON date format '/Date(timestamp_ms)/'.

        Args:
            date_string (str): The date string from the API response.

        Returns:
            str: Date formatted as 'YYYY-MM-DD', or None if parsing fails.
        """
        if not date_string or not date_string.startswith('/Date('):
            return None
        try:
            timestamp_ms = int(date_string.strip('/Date()'))
            dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
            return dt_object.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            print(f"Warning: Could not parse date string: {date_string}")
            return None

    def _process_permit_data(self, permit_json):
        """
        Extracts and formats relevant data from a single permit JSON object.

        Args:
            permit_json (dict): A dictionary representing a single permit.

        Returns:
            dict: A dictionary containing the desired fields, or None if essential data is missing.
        """
        permit_number = permit_json.get("TypeNumber")
        address = permit_json.get("PermitAddress")
        contractor = permit_json.get("ProfessionalName")
        issued_date_raw = permit_json.get("IssuedDate")
        issued_date = self._parse_date(issued_date_raw)
        permit_type = permit_json.get("PermitType") # e.g., RPV
        status = permit_json.get("Status") # e.g., OPEN, DONE

        # Basic validation - require at least permit number and address
        if not permit_number or not address:
            return None

        return {
            "permit_number": permit_number,
            "address": address,
            "contractor": contractor,
            "issued_date": issued_date,
            "permit_type": permit_type,
            "status": status
            # Add other fields if needed: PID, ProjectDescription, Parcel etc.
        }

    def fetch_permits_for_date_range(self, start_date_str, end_date_str, delay_seconds=1):
        """
        Fetches all solar permits within a given date range, handling pagination.

        Args:
            start_date_str (str): Start date in 'MM/DD/YYYY' format.
            end_date_str (str): End date in 'MM/DD/YYYY' format.
            delay_seconds (int): Seconds to wait between paginated requests.

        Returns:
            list: A list of dictionaries, where each dictionary represents a processed permit.
        """
        all_permits = []
        current_page = 1
        total_records = 0 

        print(f"Fetching solar permits from {start_date_str} to {end_date_str}...")

        while True:
            print(f"  Requesting page {current_page}...")
            payload = {
                'sort': '',
                'page': current_page,
                'pageSize': self.page_size,
                'group': '',
                'filter': '',
                'PermitType': '',
                'PermitNumber': '',
                'TempPermit': 'Y',
                'AddrNumber': '',
                'AddrDirection': '',
                'AddrStreet': '',
                'AddrType': '',
                'ProfName': '',
                'ProfStateLicense': '',
                'ProjectNumber': '',
                'ProjectName': '',
                'SolarGreenAdaptive': 'solar',
                'SolarGreenAdaptiveStartDate': start_date_str,
                'SolarGreenAdaptiveEndDate': end_date_str
            }

            response_data = self._make_request(payload)

            if response_data is None:
                print(f"  Failed to retrieve data for page {current_page}. Stopping.")
                break # Stop if a request fails

            permit_list_json = response_data.get("Data", [])
            if not permit_list_json and current_page > 1:
                # If we get an empty list on a page > 1, assume we're done
                print("  No more permits found on this page.")
                break

            if current_page == 1:
                total_records = response_data.get("Total", 0)
                print(f"  Total records reported by API: {total_records}")
                if total_records == 0:
                    print("  No permits found for this date range.")
                    break # No need to continue if total is 0
                elif total_records > 0 and not permit_list_json:
                     print("  API reported records, but none found on the first page. Stopping.")
                     break # Avoid infinite loop if API reports total but returns empty data

            # Process the permits found on this page
            count_on_page = 0
            for permit_json in permit_list_json:
                processed = self._process_permit_data(permit_json)
                if processed:
                    all_permits.append(processed)
                    count_on_page += 1
            print(f"  Processed {count_on_page} valid permits from this page.")

             # Check if we need to fetch the next page
            # Exit if we've processed enough permits based on the total reported
            # Or if the last page returned fewer items than page size (including zero)
            if len(all_permits) >= total_records or len(permit_list_json) < self.page_size:
                 print(f"  Fetched {len(all_permits)} permits. Reached expected total ({total_records}) or end of data.")
                 break

            # Prepare for the next page
            current_page += 1
            print(f"  Waiting {delay_seconds} seconds before next request...")
            time.sleep(delay_seconds) # Be polite to the server

        print(f"Finished fetching. Total processed permits: {len(all_permits)}")
        return all_permits
# --- Function to save data to CSV  ---
def save_to_csv(data, filename):
    """
    Saves a list of dictionaries to a CSV file.

    Args:
        data (list): A list of dictionaries.
        filename (str): The desired name for the output CSV file.
    """
    if not data:
        print("No data provided to save.")
        return

    # Use keys from the first dictionary as headers
    # Assumes all dictionaries have the same structure
    fieldnames = data[0].keys()

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader() # Write the header row
            writer.writerows(data) # Write all data rows
        print(f"Successfully saved {len(data)} records to {filename}")
    except IOError as e:
        print(f"Error writing to CSV file {filename}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV saving: {e}")

# --- Main execution block ---
if __name__ == "__main__":
   
    # Set the specific date range
    start_date_to_fetch = "03/27/2025"
    end_date_to_fetch = "04/26/2025"
    # Use a relevant identifier for the filename for this specific range
    run_date_str = "20250327-20250426"
    


    # Define the output filename
    output_filename = f"phoenix_solar_permits_{run_date_str}.csv"

    print("-" * 50)
    print("Starting Phoenix Solar Permit Scraper")
    print(f"Target Date Range: {start_date_to_fetch} to {end_date_to_fetch}") 
    print(f"Output file: {output_filename}")
    print("-" * 50)

    # Instantiate the scraper (using page size 50 is efficient)
    scraper = PhoenixPermitScraper(page_size=50)

    # Fetch the permits
    try:
        # The fetch_permits_for_date_range method handles pagination internally
        fetched_permits = scraper.fetch_permits_for_date_range(start_date_to_fetch, end_date_to_fetch)

        if fetched_permits:
            # Save the results to CSV
            save_to_csv(fetched_permits, output_filename)

            # Optionally print some results to console
            print(f"\n--- Fetched Permits ({len(fetched_permits)} total) ---")
            print("--- First 5 Fetched Permits (Preview) ---")
            for i, permit in enumerate(fetched_permits[:5]):
                 print(f"Permit #{i+1}: {permit}")
            if len(fetched_permits) > 5:
                print("...")

        else:
            print("\nNo permits were fetched or processed.")

    except Exception as e:
        print(f"\nAn error occurred during the scraping process: {e}")

    print("-" * 50)
    print("Scraping process finished.")
    print("-" * 50)