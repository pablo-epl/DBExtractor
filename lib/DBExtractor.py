import psycopg2
from psycopg2 import extras
import getpass
from lib import config

class DBExtractor(object):
    _instance = None
    _user = None
    _password = None

    def __new__(cls):
        # If there's already a connection to the database and the user tries to create a new one, instead of creating
        # a new one it will return the one that already exists
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            schema = config._schema
            db_name = config._db_name
            host = config._host_name
            port = config._host_port
            if not cls._user:
                cls._user = raw_input("User: ")
            if not cls._password:
                cls._password = getpass.getpass("Password: ")
            db_config = {'dbname': db_name, 'host': host,
                     'password': cls._password, 'port': port, 'user': cls._user}
            try:
                print('connecting to PostgreSQL database...')
                connection = DBExtractor._instance.connection = psycopg2.connect(**db_config)
                cursor = DBExtractor._instance.cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
                schema = DBExtractor._instance.schema = schema
                cursor.execute('SELECT VERSION()')
                db_version = cursor.fetchone()
            except Exception as error:
                print('Error: connection not established {}'.format(error))
            else:
                print('connection established\n{}'.format(db_version["version"]))
        return cls._instance

    def __init__(self):
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor
        self.schema = config._schema
        self.db_name = config._db_name
        self.host = config._host_name
        self.port = config._host_port

    def __del__(self):
        try:
            self.connection.close()
            self.cursor.close()
            DBExtractor._instance = None
            print ('connection terminated')
        except Exception as error:
            print str(error)

    def reconnect(self):
        # I don't know if this is the best way to handle a reconnection
        # any suggestions?
        self._instance = DBExtractor()
        self.__init__()

    def retrieve_all_from_table(self, table):
        table_content = []
        while not table_content:
            try:
                self._instance.cursor.execute("select * from {}.{}".format(self.schema, table))
                for row in self.cursor:
                    table_content.append(row)
            except Exception as error:
                # what's the best option to handle errors when the connection is lost?
                self.reconnect()
        return table_content

    def retrieve_all_from_object(self, table, id_field,object_id):
        object_data = None
        while not object_data:
            try:
                self.cursor.execute("select * from {}.{} where {} = '{}'".format(self.schema, table,
                                                                                 id_field, object_id))
                object_data = self.cursor
            except Exception as error:
                # what's the best option to handle errors when the connection is lost?
                self.reconnect()
        return object_data
