import csv
import sys

def read_csv(csv_file):
    csv_rows = []
    with open(csv_file) as csvFile:
        reader = csv.reader(csvFile, delimiter=',')
        title = reader.next()
        print(title)
        for row in reader:
            csv_row = {title[i]:row[i] for i in range(len(title))}
            write_to_elastic(csv_row)

def write_to_elastic(data):
    print (data)
if __name__ == "__main__":
    read_csv(sys.argv[1])