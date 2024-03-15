# Import necessary libraries/packages
import pandas as pd
import requests
from bs4 import BeautifulSoup, re
from dateutil.parser import parse
import mimetypes


def scrape_data_links(host, page_url):

    # Get page content
    webpage = requests.get(page_url)

    # Convert content from bytes to HTML
    soup = BeautifulSoup(webpage.content, 'html.parser')

    # Extract report/data paths
    link_tags = soup.find_all('a')

    # Obtain complete data links from host address and data paths
    data_links = []

    for tag in link_tags:
        # is_excel_file = tag.get('href').endswith('.xlsx')
        # is_report = 'report' in tag.text.lower()
        # is_data_path = is_report and is_excel_file

        pattern = r".+fatal.+report.+\.xlsx"
        is_data_path = re.search(pattern, tag.get('href').lower())


        if is_data_path:
            data_links.append(archive_host + tag.get('href'))


    return data_links


def extract_date_from_string(input_string):
    # Define a regular expression pattern to match the date format
    # date_pattern = r'\d{2}/\d{2}/\d{4}'
    date_pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'

    # Search for the date pattern in the input string
    date_match = re.search(date_pattern, input_string)

    if date_match:
        # Extract and return the matched date
        extracted_date = date_match.group()
        return extracted_date

    else:
        return None


def extract_data(data_file):
    data_columns = ['S/NO', 'TIME 24 HOURS', 'BASE/SUB BASE', 'COUNTY', 'ROAD', 'PLACE', 'MV INVOLVED', 'BRIEF ACCIDENT DETAILS', 'NAME OF VICTIM', 'GENDER', 'AGE', 'CAUSE CODE', 'VICTIM', 'NO.']

    # Skip the summary footer
    data = pd.read_excel(data_file, skipfooter=6, names=data_columns)

    # Extract date from report title(string)
    title_str = data.iloc[0, 0]
    file_date = ""


    if title_str:
        file_date = extract_date_from_string(title_str)

    # Drop empty rows
    data.dropna(how='all', inplace=True)

    # Add report date
    data['DATE'] = file_date

    # Return data excluding the first two irrelevant rows
    return data.iloc[2:]


def generate_dataset(links_list, save_dataset=False, data_path='dataset.csv'):
    file_count = 0
    no_file_count = 0
    all_columns = ['DATE', 'TIME 24 HOURS', 'BASE/SUB BASE', 'COUNTY', 'ROAD', 'PLACE', 'MV INVOLVED', 'BRIEF ACCIDENT DETAILS', 'GENDER', 'AGE', 'CAUSE CODE', 'VICTIM', 'NO.']
    valid_links = []
    
    # Create Empty dataframe for dataset
    dataset = pd.DataFrame(columns=all_columns)

    for link in links_list:
        try:
            response = requests.get(link)
        
            # Get the content type from the response headers
            content_type = response.headers.get('content-type')

            # Use mimetypes module to guess the file extension based on content type
            not_excel_file = mimetypes.guess_extension(content_type) != '.xlsx'
            
            if not_excel_file:
                # print("not ok")
                no_file_count += 1
                continue
            
            else:
                file_count += 1
                new_data = extract_data(response.content)

                dataset = pd.concat([dataset, new_data])

        except Exception as e:
            # print(f"Exception occurred: {e} \n Skipping to next item.")
            print(f"Exception occurred! \n Skipping to next item.")
            continue
        
        finally:
            print(f"Loop {links_list.index(link) + 1} complete")

    
    print(f"Total Invalid Files: {no_file_count}")
    print(f"Total Valid Files: {file_count}")

    # Save dataset as CSV
    dataset.to_csv("dataset.csv", header=True, index=False)
    
    if save_dataset:
        # Save dataset as CSV
        dataset.to_csv(data_path, header=True, index=False)


    return dataset, valid_links


def load_text_as_list(file_path):
    data_list = []

    with open(file_path, 'r') as file:
        for line in file:
            # Convert each line to a string and append to the list
            data_list.append(str(line.strip()))

    return data_list
	
	
def save_list_as_text(data, file_path='data.txt'):
    with open(file_path, 'w') as file:
        for item in data:
            file.write(str(item) + '\n')


def main():
	archive_host = "https://web.archive.org/web/20181216075237/http://www.ntsa.go.ke/"
	archive_URL = "https://web.archive.org/web/20181216075237/http://www.ntsa.go.ke/index.php?option=com_content&view=article&id=237"

	# Get links to data direct from archive 
	# data_links = scrape_data_links(archive_host, archive_URL)

	# Get links to data "scraped and verified" from archive 
	data_links = load_text_as_list('valid_links.txt')

	print(f"Total Links: {len(data_links)}")
	
	# Generate data
	dataset, valid_links = generate_dataset(data_links, save_dataset=True)
	
	# Save_valid_links
	# save_list_as_text(valid_links, file_path='valid_links.txt')
	
	


if __name__ == '__main__':
    main()


