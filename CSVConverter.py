import csv
from decimal import Decimal


def convert_to_decimal(value):
    try:
        # replace dot by comma
        value = value.replace('.', ',')
        # Convert to Decimal and then to string
        return value
    except Exception as e:
        print(f"Error converting value '{value}': {e}")
        return value  # Return the original value if conversion fails



path = 'C:\\Users\\henry\\Dropbox\\IT\\Funnel Units\\Test\\Data\\Acrolinx\\2024-09\\sf Export\\'
#input_file = path+'accounts.csv'
#output_file = path+'accounts_output.csv'

#input_file = path+'opportunities.csv'
#output_file = path+'opportunities_output.csv'

input_file = path+'oppHistory.csv'
output_file = path+'oppHistory_output.csv'
decimal_columns = ['Amount__c', 'Weighted_Amount__c', 'ARR_Amount__c', 'Weighted_ARR_Amount__c']

with open(input_file, 'r', newline='', encoding='utf-16') as csv_in:
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_out:
        reader = csv.DictReader(csv_in, delimiter=',')
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(csv_out, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()

        for row in reader:
            for col in decimal_columns:
                if col in row and row[col]:
                    row[col] = convert_to_decimal(row[col])
            writer.writerow(row)

