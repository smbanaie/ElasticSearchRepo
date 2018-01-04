import csv
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers


es = Elasticsearch()


def insert_from_csv_regular(csv_file):
    counter = 0
    with open(csv_file,"r") as csvFile:
        reader = csv.reader(csvFile, delimiter=',')
        if sys.version_info[0] < 3:
            # python 2
            title = reader.next()
        else:
            # in python 3
            title = next(reader)
        print(title)
        for row in reader:
            csv_row = {title[i]:row[i] for i in range(len(title))}
            if sys.argv[2] == "movies" :
                prepare_data(csv_row)
            res = es.index(index=sys.argv[2], doc_type=sys.argv[3], id=csv_row[sys.argv[4]], body=csv_row)
            counter = counter + 1
            if counter % 10 == 0:
                print("Inserted So Far : " + str(counter))
                print("Last Record Inserted : ")
                print(csv_row)


def insert_from_csv_bulk(csv_file):

    with open(csv_file,"r") as csvFile:
        reader = csv.reader(csvFile, delimiter=',')

        if sys.version_info[0] < 3:
        #python 2
            title = reader.next()
        else :
        # in python 3
            title =next(reader)
        print(title)
        bulk_cnt = 1

        for row in reader:

            csv_row = {title[i]:row[i] for i in range(len(title))}
            if sys.argv[2] == "movies" :
                prepare_data(csv_row)
            action = {
                "_index": sys.argv[2],
                "_type": sys.argv[3],
                "_id": csv_row[sys.argv[4]],
                "_source": csv_row
            }

            bulk_cnt +=1
            if bulk_cnt % 100 == 0:
                print("Inserted So Far : " + str(bulk_cnt))

            yield action



def prepare_data(row) :
    if ("Comedy" in row["genres"]) :
        row["Comedy"] = 1
    else :
        row["Comedy"] = 1

    if ("Adventure" in row["genres"]):
        row["Adventure"] = 1
    else:
        row["Adventure"] = 0

    if ("Fantasy" in row["genres"]):
        row["Fantasy"] = 1
    else:
        row["Fantasy"] = 0

    if ("Horror" in row["genres"]):
        row["Horror"] = 1
    else:
        row["Horror"] = 0

    if ("Animation" in row["genres"]):
        row["Animation"] = 1
    else:
        row["Animation"] = 0

    if ("Children" in row["genres"]):
        row["Children"] = 1
    else:
        row["Children"] = 0

    if ("Drama" in row["genres"]):
        row["Drama"] = 1
    else:
        row["Drama"] = 0

    if ("Romance" in row["genres"]):
        row["Romance"] = 1
    else:
        row["Romance"] = 0

    if ("Thriller" in row["genres"]):
        row["Thriller"] = 1
    else:
        row["Thriller"] = 0

    if ("Sci-Fi" in row["genres"]):
        row["Sci-Fi"] = 1
    else:
        row["Sci-Fi"] = 0

    if ("War" in row["genres"]):
        row["War"] = 1
    else:
        row["War"] = 0

    if ("Action" in row["genres"]):
        row["Action"] = 1
    else:
        row["Action"] = 0

    if ("Western" in row["genres"]):
        row["Western"] = 1
    else:
        row["Western"] = 0

    if ("Documentary" in row["genres"]):
        row["Documentary"] = 1
    else:
        row["Documentary"] = 0

    if ("Mystery" in row["genres"]):
        row["Mystery"] = 1
    else:
        row["Mystery"] = 0

    if ("Crime" in row["genres"]):
        row["Crime"] = 1
    else:
        row["Crime"] = 0

    if ("Musical" in row["genres"]):
        row["Musical"] = 1
    else:
        row["Musical"] = 0

    try :
        row["year"] = int(row["title"][row["title"].rfind("(") + 1:row["title"].rfind(")")])
    except :
        print ("No Year Found" )



if __name__ == "__main__":

    # Run this file in this manner  :  csvfile_address , index_name,type_name , id_field_in_header

    # For Insert Row By Row - inefficient
    #insert_from_csv_regular(sys.argv[1])

    success, _ = helpers.bulk(es,insert_from_csv_bulk(sys.argv[1]))
    print("Done! - Succes : "+ str(success))
