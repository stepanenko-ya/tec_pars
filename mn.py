from tabQ import TecDoc
import os
import pymssql
import py7zr
import shutil
import logging
import math
from sys import argv

conn = pymssql.connect(
    server="10.175.1.60:1433",
    user="importer_doc",
    password='QAZxsw123',
    database="Test")
db = conn.cursor()

field = "DLNr"                              # if previous version was Q1 or Q2 change to "DLNr"


def create_table(version, table_num, arch_file):
    tab = tecdoc_parse.tables(version, table_num)
    column_name = ""
    for col in tab:
        column_name += col["name"] + " NVARCHAR(" + str(col["length"]) + "), "
    try:
        command_create = "CREATE TABLE  t" + table_num + " (" + column_name[:-2] + ")"
        db.execute("IF (OBJECT_ID('t" + table_num + "') IS NULL) EXEC('" + command_create + "')")
        conn.commit()

        if db.fetchone():
            db.execute(f"TRUNCATE TABLE t{table_num}")

        conn.commit()
    except(pymssql.OperationalError, pymssql.ProgrammingError, KeyError):
        log(f"Incorrect syntax in {table_num}, archive {arch_file}")

    return table_num


def file_parsing(version, unpack_file):
    val = []
    path_to_file = "unpacked_data" + "/" + unpack_file
    table_number = unpack_file[:3]

    file = open(path_to_file, 'r')
    print("Запись данных в ", unpack_file[:3], " таблицу")
    for row in file:
        row = row.replace("'", "")
        row = row.replace(",", "")
        step = 0
        string_data = ""

        tab = tecdoc_parse.tables(version, table_number)
        for column in tab:
            data = row[step:step + column["length"]]
            step += column["length"]
            string_data += data + ","
        list_data = string_data[:-1].split(",")
        val.append(tuple(list_data))
    file.close()
    return val


def run_sql(version, table_number, values):
    tab = tecdoc_parse.tables(version, table_number)

    col_numbers = "%s, " * len(tab)
    replacement = col_numbers[0:-2]
    step = 0
    nxt = 500

    sql = "insert into t" + table_number + " values(" + replacement + ")"

    cycle = math.ceil(len(values) / nxt)

    try:
        conn = pymssql.connect(server="10.175.1.60:1433", user="importer_doc", password='QAZxsw123', database="Test")
        db = conn.cursor()
        try:
            for _ in range(1, cycle + 1):
                db.executemany(sql, values[step:step + nxt])
                conn.commit()
                step += nxt
        except pymssql.ProgrammingError:
            log(f"Incorrect syntax in {arch_file} t{table_number}, slice {step}")
    finally:
        conn.close()
    return "ok"


def log(error_message):
    logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s] %(message)s', level=logging.DEBUG, filename=u'mylog.log',
                        datefmt='%d-%m-%Y %H:%M:%S')
    logging.error(error_message)


def main(unpak_files, arch_file):
    t = argv
    version = t[1]
    for file in unpak_files:
        if file[-3:] != "GIF":
            try:
                table = create_table(version, file[:3], arch_file)
                values = file_parsing(version, file)
                run_sql(version, table, values)
            except KeyError:
                log(f"Table {file[-3:]} does not exist")
    shutil.rmtree("unpacked_data")


if __name__ == '__main__':
    tecdoc_parse = TecDoc()

    archive_direct = os.listdir("archives")
    for arch_file in archive_direct:
        print("Разархивация файлов...", arch_file)
        try:
            archive = py7zr.SevenZipFile("archives/" + arch_file, mode='r')
            archive.extract("unpacked_data")
            all_files = os.listdir("unpacked_data")
            main(all_files, arch_file)
        except py7zr.exceptions.Bad7zFile:
            log("Bad file " + arch_file)
