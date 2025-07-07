import csv

class SltErrorsStatsCsv:
    def __init__(self, output_file: str):
        self.output_file = output_file

    def prepare(self):
        # Create a basic CSV with test file information
        with open(self.output_file, 'w', newline='') as csvfile:  
            writer = csv.writer(csvfile)
            writer.writerow(['filename', 'statement', 'error', 'error_stack_trace'])
    
    def add_stats_row(self, filename: str, statement: str, error: str, advanced_error: str):
        with open(self.output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile,
                        delimiter=',',
                        quotechar='"',
                        quoting=csv.QUOTE_MINIMAL,
                        lineterminator='\n',
                        escapechar='\\')
            writer.writerow([filename, statement, error, advanced_error])
    
