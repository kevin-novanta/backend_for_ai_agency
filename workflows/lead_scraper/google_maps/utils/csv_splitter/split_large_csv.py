import csv
import os

def split_csv(input_file_path, output_dir, max_rows=1000):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(input_file_path, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        headers = next(reader)

        file_count = 1
        current_outfile = open(os.path.join(output_dir, f'split_output_{file_count}.csv'), mode='w', newline='', encoding='utf-8')
        writer = csv.writer(current_outfile)
        writer.writerow(headers)
        current_row_count = 0

        for row in reader:
            if current_row_count >= max_rows:
                current_outfile.close()
                file_count += 1
                current_outfile = open(os.path.join(output_dir, f'split_output_{file_count}.csv'), mode='w', newline='', encoding='utf-8')
                writer = csv.writer(current_outfile)
                writer.writerow(headers)
                current_row_count = 0

            writer.writerow(row)
            current_row_count += 1

        current_outfile.close()


input_file = '/Users/kevinnovanta/backend_for_ai_agency/workflows/lead_scraper/utils/csv_splitter/Input_CSV/enriched_data.csv'
output_dir = '/Users/kevinnovanta/backend_for_ai_agency/workflows/lead_scraper/utils/csv_splitter/Outputted_CSV'

split_csv(input_file, output_dir, max_rows=1000)