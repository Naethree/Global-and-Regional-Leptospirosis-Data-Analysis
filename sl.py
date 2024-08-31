import requests
from bs4 import BeautifulSoup
import os
import pdfplumber
import pandas as pd
from datetime import datetime

# Define the URL of the webpage
url = 'https://www.epid.gov.lk/weekly-epidemiological-report/weekly-epidemiological-report'

# Define the directory to save downloaded PDFs
output_dir = '/home/naethree/Users/naethree/airflow/dags/slpdfs'

# Define the base path for CSV files
csv_base_path = '/home/naethree/Users/naethree/airflow/dags/slcsv/'

# Ensure the directory exists
os.makedirs(output_dir, exist_ok=True)
os.makedirs(csv_base_path, exist_ok=True)

def download_last_pdf(url, output_dir):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all accordions
    accordions = soup.find_all(class_='accordions')

    downloaded_pdfs = []

    for accordion in accordions:
        # Find all content within the accordion
        content = accordion.find(class_='content')
        if content:
            # Find all product links within the content
            products = content.find_all('li', class_='product')
            if products:
                # Get the last product link
                last_product = products[-1]
                a_tag = last_product.find('a', href=True)
                if a_tag:
                    pdf_url = a_tag['href']
                    pdf_name = os.path.basename(pdf_url)
                    pdf_path = os.path.join(output_dir, pdf_name)

                    # Download the PDF
                    pdf_response = requests.get(pdf_url)
                    with open(pdf_path, 'wb') as file:
                        file.write(pdf_response.content)

                    print(f'Downloaded: {pdf_path}')
                    downloaded_pdfs.append(pdf_path)

    return downloaded_pdfs

def extract_table_from_pdf(pdf_path, table_title):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if table_title in text:
                tables = page.extract_tables()
                print('Table found')

                combined_data = pd.DataFrame()

                for table in tables:
                    df = pd.DataFrame(table[1:], columns=table[0])

                    # Handle different structures based on the presence of specific columns
                    if 'DPDHS\nDivision' in df.columns:
                        df = df.iloc[1:27, 12]  # Skip header rows
                    else:
                        df = df.iloc[3:29, 12]  # Skip header rows

                    df.reset_index(drop=True, inplace=True)
                    combined_data = pd.concat([combined_data, df], ignore_index=True, sort=False)

                return combined_data

    return None

def create_latest_csv():
    today = datetime.now()
    current_year = today.year

    # Define the year to check
    target_year = current_year - 1

    # Download the PDF and extract data
    pdf_paths = download_last_pdf(url, output_dir)
    table_title = 'Selected notifiable diseases reported by Medical Officers of Health'
    unrotated_pdfs = pdf_paths[12:19][::-1]
    all_combined_data = pd.DataFrame()

    for pdf_path in unrotated_pdfs:
        extracted_data = extract_table_from_pdf(pdf_path, table_title)
        if extracted_data is not None:
            all_combined_data = pd.concat([all_combined_data, extracted_data], ignore_index=True, sort=False)
            print(f"Extracted data from {pdf_path}:")
            print(extracted_data)
        else:
            print(f"No matching table found in {pdf_path}")

    all_combined_data.columns = ['Cases']
    unrotated_combined_data = split_and_expand(all_combined_data.copy(), 'Cases')

    if len(unrotated_combined_data) > 53:
        unrotated_combined_data = unrotated_combined_data.drop([52, 53])
    unrotated_combined_data = unrotated_combined_data.reset_index(drop=True)
    unrotated_final_df = split_into_columns(unrotated_combined_data['Cases'], 26)

    rotated_paths = pdf_paths[0:12][::-1]
    rotated_combined_data = pd.DataFrame()

    for pdf_path in rotated_paths:
        extracted_data = extract_table_from_pdf_rotated(pdf_path, table_title)
        if extracted_data is not None:
            rotated_combined_data = pd.concat([rotated_combined_data, extracted_data], ignore_index=True, sort=False)
            print(f"Extracted data from {pdf_path}:")
            print(extracted_data)
        else:
            print(f"No matching table found in {pdf_path}")

    rotated_combined_data.columns = ['Cases']
    rotated_combined_data = split_and_expand(rotated_combined_data.copy(), 'Cases')
    rotated_final_df = split_into_columns(rotated_combined_data['Cases'], 26)
    complete_df = unrotated_final_df.merge(rotated_final_df, left_index=True, right_index=True)

    Regions = ['Colombo', 'Gampaha', 'Kalutara', 'Kandy', 'Matale', 'Nuwara Eliya', 'Galle', 'Hambantota', 'Matara',
               'Jaffna', 'Kilinochchi', 'Mannar', 'Vavuniya', 'Mullaitivu', 'Batticaloa', 'Ampara', 'Trincomalee',
               'Kurunegala', 'Puttalam', 'Anuradhapura', 'Polonnaruwa', 'Badulla', 'Monaragala', 'Ratnapura', 'Kegalle']

    complete_df.insert(0, 'Region', Regions)
    complete_df['Year'] = target_year

    csv_path = os.path.join(csv_base_path, f'leptospirosis_data_{target_year}.csv')
    complete_df.to_csv(csv_path, index=False)
    print(f'CSV file for {target_year} saved to {csv_path}')

# Run the function to create the latest CSV
create_latest_csv()
