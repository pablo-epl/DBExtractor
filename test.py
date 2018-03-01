from lib import DBExtractor

db_connection = DBExtractor.DBExtractor()
example_table_data = db_connection.retrieve_all_from_table("example")

for row in example_table_data:
    print row
    break

db_connection.__del__()