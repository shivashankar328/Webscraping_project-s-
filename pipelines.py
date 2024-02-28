# # Define your item pipelines here
# #
# # Don't forget to add your pipeline to the ITEM_PIPELINES setting
# # See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

                   
from itemadapter import ItemAdapter
import mysql.connector
from scrapy.exceptions import DropItem
from scrapy.utils.project import get_project_settings

class NasdaqPipeline:
    def __init__(self):
        self.settings = get_project_settings()
        self.connect_mysql()
    
    def connect_mysql(self):
        """
        getting the database connection details from setting file.
        :return: connect to mysql database
        """
        try:
            self.cnx = mysql.connector.connect(
                host=self.settings['MYSQL_HOST'],
                port=self.settings['MYSQL_PORT'],
                database=self.settings['MYSQL_DATABASE'],
                user=self.settings['MYSQL_USER'],
                password=self.settings['MYSQL_PASSWORD']
            )
            self.cursor = self.cnx.cursor()
        except mysql.connector.Error as err:
            print('connection error is:', err)
    
    def create_table_if_not_exists(self, item):
        """

        :param item: scrpaed item keys and values from nasdaqresults.py in spider folder
        :return: creating table in databse if not exists using item keys.
        """
        try:
            self.table = 'Nasdaq_historical_data'
            col_names = ', '.join([f'{key} VARCHAR(255)' for key in item.keys()])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table} ({col_names})"
            self.cursor.execute(create_table_query)
            self.cnx.commit()
        except mysql.connector.Error as err:
            print("Error: {}".format(err))
            self.cnx.rollback()
        
    def close_spider(self, spider):
        """
        :return: closing the connection and cursor after execution completed.
        """
        try:
            self.cursor.close()
            self.cnx.close()
        except mysql.connector.Error as err:
            print('mysql closing error:', err)
            
    def process_item(self, item, spider):
        """

        :param item: output from nasdaqresults.py file in dictionary format for a user input search

        :return: creating table in database and inserting the output dictionary in mysql table
        """
        try:
            self.create_table_if_not_exists(item)
            self.insert_to_mysql(item)
        except mysql.connector.Error as err:
            print('processing error:', err)
            raise DropItem(f"failed to insert item into table:{err}")
        
        return item

    def insert_to_mysql(self, item):
        """

        :param item: output results in dictionary format
        :return: creating column names from keys and placeholder for each value.
        executing the insert query and commit the tabel
        """
        columns = ', '.join(item.keys())
        placeholder = ', '.join(['%s'] * len(item))
        sql_query = f"INSERT INTO {self.table} ({columns}) VALUES ({placeholder})"
        values = tuple(item.values())
        
        try:
            self.cursor.execute(sql_query, values)
            self.cnx.commit()
        except mysql.connector.Error as err:
            print("error as:", err)
            self.cnx.rollback()
            raise DropItem(f"failed to insert item into sql: {err}")
