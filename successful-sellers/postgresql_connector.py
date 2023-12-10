import psycopg2


class Connector:
    def __init__(self):
        self.connection = None

    def create_connection(self):

        try:
            self.connection = psycopg2.connect(database="postgres",
                                               user='postgres',
                                               password='yourpassword',
                                               host='127.0.0.1',
                                               port='5432')

            self.connection.autocommit = True
        except psycopg2.Error as error:
            print("Failed to connection ", error)

        return self.connection

    def close_connection(self):
        self.connection.close()
        return None
