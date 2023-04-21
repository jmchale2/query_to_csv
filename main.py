import os
from dotenv import load_dotenv

# import pyodbc
from pathlib import Path
import csv
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--parameters", help="parameter values parsed in order listed in query")
    parser.add_argument("--in_file", help="path to input file, default is ./query.sql", default="./query.sql")
    parser.add_argument(
        "--out_file", help="path to output file, default is ./results.csv", default="./results.csv"
    )

    load_dotenv()

    args = parser.parse_args()

    print(f"{args.parameters=}")
    print(f"{args.in_file=}")
    print(f"{args.out_file=}")

    if args.parameters is not None:
        params = args.parameters.split(",")

    if args.in_file is None:
        query_path = Path("./query.sql").resolve()
    else:
        query_path = Path(args.in_file).resolve()

    if args.out_file is None:
        output_path = Path("./results.csv").resolve()
    else:
        output_path = Path(args.out_file).resolve()

    connection_string = f"DRIVER={os.environ['DRIVER']};SERVER={os.environ['SERVER']};DATABASE={os.environ['DATABASE']};UID={os.environ['UID']};PWD={os.environ['PWD']}"

    with query_path.open("r") as f:
        query = f.read()

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(query, params)

    results = cursor.fetchall()
    colnames = [x[0] for x in cursor.description]
    cursor.close()
    conn.close()

    query_data = [tuple(colnames)] + results

    with output_path.open("w", newline="") as query_csv:
        querywriter = csv.writer(query_csv, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        for row in query_data:
            querywriter.writerow(row)
