import pandas as pd
from django.db import connection
from upload.managers.exception import InterruptException
import ast, csv

def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, float]:
        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
            # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'int'
            else:
                return 'bigint'
    if type(t) is float and current_type not in ['varchar']:
        return 'decimal'
    else:
        return 'varchar'

class UploadManager:
    """ An upload manager class which interacts with the database"""

    def __init__(self, table_name, file_name="./csv data/set1.csv"):
        """Constructor to intitalize some properties.

        Args:
            user_id : id of the user uploading file. 
            file_name: Name of file being uploaded.
        """
        self.file_name = file_name
        self.table_name = table_name
        self.lines_read = 0
        self.is_paused = False
        self.is_terminated = False
        self.progress = 0
        self.headers = ""
        self.total_rows = 0
        super().__init__()

    def create_table(self):
        """Method to create a table and save to the database.

        Raises:
            Conflict: If a table with same name already exists.
        """
        try:
            df = pd.read_csv(self.file_name, skiprows=self.lines_read)
            self.headers = df.columns.to_list()
            tmp = ""
            for i in self.headers:
                if len(tmp) != 0:
                    tmp += ","
                if len(str(i).split(" ")) == 1:
                    tmp += str(i)
                else:
                    tmp += str(i).replace(" ","_")
            self.headers = tmp
            c = connection.cursor()
            f = open(self.file_name, 'r',encoding='utf-8')
            read = csv.reader(f)
            headers, type_list = [], []
            for row in read:
                if len(headers) == 0:
                    headers = row
                    for col in row:
                        type_list.append('')
                else:
                    for i in range(len(row)):
                        # NA is the csv null value
                        if type_list[i] == 'varchar' or row[i] == 'NA':
                            pass
                        else:
                            var_type = dataType(row[i], type_list[i])
                            type_list[i] = var_type
            f.close()
            statement = f"create table {self.table_name} ("
            for i in range(len(headers)):
                if type_list[i] == 'varchar':
                    statement = (statement + '\n{} varchar({}),').format(headers[i].lower().replace(" ","_"), str(256))
                else:
                    statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower().replace(" ","_"), type_list[i])

            statement = statement[:-1] + ');'
            c.execute(statement)
        finally:
            c.close()

    def start(self):
        """
        Method to start uploading rows of csv file into database

        Raises:
            InterruptException: When the upload is paused or terminated. 
        """
        c = connection.cursor()

        self.is_paused = False
        self.is_terminated = False

        df = pd.read_csv(self.file_name, skiprows=self.lines_read)
        rows_list = [list(row) for row in df.values]

        if self.lines_read == 0:
            self.create_table()
            self.total_rows = len(df)

        for row in rows_list:
            try:
                tmp = ""
                for i in row:
                    if len(tmp) != 0:
                        tmp += ","
                    tmp += "'" + str(i) + "'"
                row = tmp
                query = f"INSERT INTO {self.table_name}({self.headers}) VALUES({row});"
                c.execute(query)
                self.lines_read += 1
                self.progress = self.lines_read / self.total_rows * 100
                status = self.check_status()
                if status:
                    raise InterruptException
            except InterruptException:
                break

    def pause(self):
        """
        Method to pause upload of rows from csv file into database. 
        """
        self.is_paused = True

    def resume(self):
        """
        Method to resume upload of rows from csv file into database. 
        """
        if self.is_terminated:
            return
        self.is_paused = False
        self.start()

    def check_status(self):
        """
        Method to check pause/terminate status.  
        """
        return self.is_paused or self.is_terminated

    def terminate(self):
        """
            Method to Rollback all the entries till now in the database. 
        """
        c = connection.cursor()
        self.is_terminated = True
        query = f"DROP TABLE IF EXISTS {self.table_name}"
        c.execute(query)

    def get_progress(self):
        """
            Method to get percentage completion of upload.
        """
        return self.progress

    def table_exists(self):
        c = connection.cursor()
        try:
            query = f"SELECT COUNT(*) from {self.table_name}"
            c.execute(query)
            return True
        except:
            return False
    
            
