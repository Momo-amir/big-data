import csv
import glob
import os
import shutil
from modules.database import ensure_database, get_connection
from modules.security import encrypt


def load_dataframe_to_mysql(df, table_name='iris_setosa'):
    ensure_database()

    conn = get_connection()
    cursor = conn.cursor()

    columns = df.columns
    column_defs = ', '.join(f"`{col}` VARCHAR(255)" for col in columns)

    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    cursor.execute(f"CREATE TABLE `{table_name}` ({column_defs})")

    rows = [
        tuple(encrypt(str(v)) if v is not None else None for v in row)
        for row in df.collect()
    ]
    if rows:
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in columns)}) VALUES ({placeholders})"
        cursor.executemany(insert_sql, rows)

    conn.commit()
    cursor.close()
    conn.close()


def save_csv(df, output_path, enc=encrypt):
    tmp_dir = output_path + '_tmp'
    df.coalesce(1).write.csv(tmp_dir, header=True, mode='overwrite')

    part_file = glob.glob(os.path.join(tmp_dir, 'part-*.csv'))[0]

    with open(part_file, newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        rows = [[enc(cell) for cell in row] if enc else list(row) for row in reader]

    with open(output_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        writer.writerows(rows)

    shutil.rmtree(tmp_dir)
