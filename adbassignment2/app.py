import textwrap

from flask import Flask, render_template, request
from werkzeug.utils import secure_filename
import pandas as pd
import pyodbc
import numpy as np

app = Flask(__name__)

driver = '{ODBC Driver 17 for SQL Server}'
server_name = 'anonymous'
database_name = 'csvdatabase'
username = "anonymous"
password = "Yash@3277"
server = '{server_name}.database.windows.net,1433'.format(server_name=server_name)
connection_string = textwrap.dedent('''
    Driver={driver};
    Server={server};
    Database={database};
    Uid={username};
    Pwd={password};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
'''.format(
    driver=driver,
    server=server,
    database=database_name,
    username=username,
    password=password
))
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        f = request.files['csvupload']
        f.save(secure_filename(f.filename))
        df = pd.read_csv(f.filename)
        print(df)
        columns = df.columns
        print(df['magError'])
        df['mag'] = df['mag'].fillna(0.0)
        df['magType'] = df['magType'].fillna("NA")
        df['nst'] = df['nst'].fillna(0)
        df['gap'] = df['gap'].fillna(0.0)
        df['dmin'] = df['dmin'].fillna(0.0)
        df['horizontalError'] = df['horizontalError'].fillna(0.0)
        df['magError'] = df['magError'].fillna(0.0)
        df['magNst'] = df['magNst'].fillna(0.0)
        print(columns[0], columns[21])
        cursor.execute('CREATE TABLE earthquake_data(Time nvarchar(50), Latitude float, Longitude float, Depth float, Mag float NULL DEFAULT 0.0, Magtype nvarchar(50) NULL DEFAULT 0.0, Nst int NULL DEFAULT 0.0, Gap float NULL DEFAULT 0.0, Dmin  float NULL DEFAULT 0.0, Rms float, Net nvarchar(50), ID nvarchar(50), Updated nvarchar(50), Place nvarchar(MAX), Type nvarchar(50), HorizontalError float NULL DEFAULT 0.0, DepthError float, MagError float NULL DEFAULT 0.0, MagNst int NULL DEFAULT 0.0, Status nvarchar(50), LocationSource nvarchar(50), MagSource nvarchar(50))')

        for row in df.itertuples():
            print(row)
            cursor.execute("INSERT INTO csvdatabase.dbo.earthquake_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.time, row.latitude, row.longitude, row.depth, row.mag, row.magType, row.nst, row.gap, row.dmin, row.rms, row.net, row.id, row.updated, row.place, row.type, row.horizontalError, row.depthError, row.magError, row.magNst, row.status, row.locationSource, row.magSource)

        conn.commit()
        return 'file uploaded successfully'
    return render_template("index.html")


if __name__ == '__main__':
    app.run()
