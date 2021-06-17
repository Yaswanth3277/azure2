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


@app.route('/eqcount', methods=['GET', 'POST'])
def eq_count():
    earthquakes = []
    if request.method == 'POST':
        magnitude = request.form.get('count')
    cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag > ?", magnitude)
    for data in cursor:
        earthquakes.append(data)
    earthquake_len = len(earthquakes)
    return render_template('eq_count.html', earthquakes = earthquakes, length = earthquake_len)


@app.route('/rangecount', methods=['GET', 'POST'])
def range_count():
    earthquakes = []
    if request.method == 'POST':
        startmag = request.form.get('startrange')
        stopmag = request.form.get('stoprange')
        timing = request.form.get('timerange')
        print(startmag, stopmag, timing)

        if timing == "Recent Week":
            time = '2021-06-05T00:00:00.000Z'
            cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag > ? and Mag < ? and Time > ?",startmag, stopmag, time)
            for data in cursor:
                earthquakes.append(data)
        else:
            cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag > ? and Mag < ?", startmag, stopmag)
            for data in cursor:
                earthquakes.append(data)
        earthquake_len = len(earthquakes)
    return render_template('range_count.html', earthquakes = earthquakes, length = earthquake_len)


@app.route('/eqlocation', methods=['GET', 'POST'])
def eq_location():
    eq_area = []
    cursor.execute("SELECT Area = right(rtrim([place]),charindex(' ',reverse(rtrim([place]))+' ')-1) From earthquake_data")
    for data in cursor:
        for value in data:
            eq_area.append(value)
    eq_area_list = list(set(eq_area))
    return render_template('eq_location.html', drop_down = eq_area_list)


@app.route('/eqoutput', methods=['GET', 'POST'])
def eq_output():
    earthquakes = []
    eq_area = []
    if request.method == 'POST':
        distance = request.form.get('dist')
        area = request.form.get('areas')
    cursor.execute("SELECT id ,latitude, longitude, place, Area = right(rtrim([place]),charindex(' ',reverse(rtrim([place]))+' ')-1) From earthquake_data where right(rtrim([place]),charindex(' ',reverse(rtrim([place]))+' ')-1)= ? AND CAST (SUBSTRING (place,0,PATINDEX('%km%',place)) as INT) >=?", area, distance)
    for data in cursor:
        earthquakes.append(data)
    earthquake_len = len(earthquakes)
    cursor.execute("SELECT Area = right(rtrim([place]),charindex(' ',reverse(rtrim([place]))+' ')-1) From earthquake_data")
    for data in cursor:
        for value in data:
            eq_area.append(value)
    eq_area_list = list(set(eq_area))
    return render_template('eq_location.html', earthquakes = earthquakes, length = earthquake_len, drop_down = eq_area_list)


@app.route('/earthquakeclusters', methods=['GET', 'POST'])
def earthquake_clusters():
    earthquakes1 = []
    earthquakes2 = []
    earthquakes3 = []
    earthquakes4 = []
    earthquakes5 = []
    earthquakes6 = []
    earthquakes7 = []
    earthquakes8 = []
    earthquakes9 = []
    earthquakes10 = []
    if request.method == 'POST':
        clusterby = request.form.get('cluster')

    if clusterby == "Magnitude":
        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>'-2.0' and Mag<'-1.0'")
        for data in cursor:
            earthquakes1.append(data)
        earthquake_len1 = len(earthquakes1)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>'-1.0' and Mag<0.0")
        for data in cursor:
            earthquakes2.append(data)
        earthquake_len2 = len(earthquakes2)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>0.0 and Mag<1.0")
        for data in cursor:
            earthquakes3.append(data)
        earthquake_len3 = len(earthquakes3)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>1.0 and Mag<2.0")
        for data in cursor:
            earthquakes4.append(data)
        earthquake_len4 = len(earthquakes4)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>2.0 and Mag<3.0")
        for data in cursor:
            earthquakes5.append(data)
        earthquake_len5 = len(earthquakes5)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>3.0 and Mag<4.0")
        for data in cursor:
            earthquakes6.append(data)
        earthquake_len6 = len(earthquakes6)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>4.0 and Mag<5.0")
        for data in cursor:
            earthquakes7.append(data)
        earthquake_len7 = len(earthquakes7)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>5.0 and Mag<6.0")
        for data in cursor:
            earthquakes8.append(data)
        earthquake_len8 = len(earthquakes8)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>6.0 and Mag<7.0")
        for data in cursor:
            earthquakes9.append(data)
        earthquake_len9 = len(earthquakes9)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Mag>7.0 and Mag<8.0")
        for data in cursor:
            earthquakes10.append(data)
        earthquake_len10 = len(earthquakes10)

        return render_template('mag_clusters.html', earthquakes1 = earthquakes1, length1= earthquake_len1, earthquakes2 = earthquakes2, length2= earthquake_len2, earthquakes3 = earthquakes3, length3= earthquake_len3, earthquakes4 = earthquakes4, length4= earthquake_len4, earthquakes5 = earthquakes5, length5= earthquake_len5, earthquakes6 = earthquakes6, length6= earthquake_len6, earthquakes7 = earthquakes7, length7= earthquake_len7, earthquakes8 = earthquakes8, length8= earthquake_len8, earthquakes9 = earthquakes9, length9= earthquake_len9, earthquakes10 = earthquakes10, length10= earthquake_len10)

    elif clusterby == 'Magnitude Type':
        types = []
        cursor.execute("select distinct Magtype from earthquake_data")
        for data in cursor:
            types.append(data)
        print(types)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[0])
        for data in cursor:
            earthquakes1.append(data)
        earthquake_len1 = len(earthquakes1)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[1])
        for data in cursor:
            earthquakes2.append(data)
        earthquake_len2 = len(earthquakes2)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[2])
        for data in cursor:
            earthquakes3.append(data)
        earthquake_len3 = len(earthquakes3)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[3])
        for data in cursor:
            earthquakes4.append(data)
        earthquake_len4 = len(earthquakes4)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[4])
        for data in cursor:
            earthquakes5.append(data)
        earthquake_len5 = len(earthquakes5)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[5])
        for data in cursor:
            earthquakes6.append(data)
        earthquake_len6 = len(earthquakes6)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[6])
        for data in cursor:
            earthquakes7.append(data)
        earthquake_len7 = len(earthquakes7)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[7])
        for data in cursor:
            earthquakes8.append(data)
        earthquake_len8 = len(earthquakes8)

        cursor.execute("select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from earthquake_data where Magtype=?",types[8])
        for data in cursor:
            earthquakes9.append(data)
        earthquake_len9 = len(earthquakes9)

        return render_template('magtype_cluster.html', earthquakes1 = earthquakes1, length1= earthquake_len1, earthquakes2 = earthquakes2, length2= earthquake_len2, earthquakes3 = earthquakes3, length3= earthquake_len3, earthquakes4 = earthquakes4, length4= earthquake_len4, earthquakes5 = earthquakes5, length5= earthquake_len5, earthquakes6 = earthquakes6, length6= earthquake_len6, earthquakes7 = earthquakes7, length7= earthquake_len7, earthquakes8 = earthquakes8, length8= earthquake_len8, earthquakes9 = earthquakes9, length9= earthquake_len9)


@app.route('/nightquake', methods=['GET', 'POST'])
def night_quake():
    earthquakes = []
    if request.method == 'POST':
        magnitude = request.form.get('night')
    cursor.execute("select  time,mag from earthquake_data where (cast(time as time) not between '08:00:00' and '18:00:00') and mag > ?", magnitude)
    for data in cursor:
        earthquakes.append(data)
    earthquake_len = len(earthquakes)
    return render_template('night_quake.html', earthquakes = earthquakes, length = earthquake_len)


if __name__ == '__main__':
    app.run()
