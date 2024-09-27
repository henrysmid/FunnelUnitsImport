import csv
from decimal import Decimal

class FUCSVConverter:

    def __init__(self, file_name):
        path = 'C:\\Users\\henry\\Dropbox\\IT\\Funnel Units\\Test\\Data\\Acrolinx\\2024-09\\sf Export\\'
        self.input_file = path + file_name+'.csv'
        self.output_file = path + file_name+'_output.csv'
        self.decimal_columns = []

    def convert_accounts(self):
        self.convert()

    def convert_opportunities(self):
        self.add_decimal_columns('Amount')
        self.convert()

    def convert_opportunity_history(self):
        self.add_decimal_columns('Amount__c')
        self.add_decimal_columns('Weighted_Amount__c')
        self.add_decimal_columns('ARR_Amount__c')
        self.add_decimal_columns('Weighted_ARR_Amount__c')
        self.convert()

    def add_decimal_columns(self,field_name):
        self.decimal_columns.append(field_name)

    def convert(self):
        with open(self.input_file, 'r', newline='', encoding='utf-16') as csv_in:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csv_out:
                reader = csv.DictReader(csv_in, delimiter=',')
                fieldnames = reader.fieldnames
                writer = csv.DictWriter(csv_out, fieldnames=fieldnames, delimiter=';')
                writer.writeheader()

                for row in reader:
                    for col in self.decimal_columns:
                        if col in row and row[col]:
                            row[col] = self.convert_to_decimal(row[col])
                    writer.writerow(row)

    def convert_to_decimal(self,value):
        try:
            # replace dot by comma
            value = value.replace('.', ',')
            # Convert to Decimal and then to string
            return value
        except Exception as e:
            print(f"Error converting value '{value}': {e}")
            return value  # Return the original value if conversion fails



#converter = FUCSVConverter('accounts')
#converter.convert_opportunities()

converter = FUCSVConverter('opportunities')
converter.convert_opportunities()

#converter = FUCSVConverter('oppHistory')
#converter.convert_opportunity_history()



