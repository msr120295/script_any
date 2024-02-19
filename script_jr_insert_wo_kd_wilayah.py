# **ETL-1:Load Data From Clickhouse**

import pandas as pd
import numpy as np
from datetime import datetime

from clickhouse_driver import Client
import sys

# Replace these values with your ClickHouse server details
clickhouse_host = '192.168.100.95'
clickhouse_port = 8547
clickhouse_database = 'jasaraharja'
clickhouse_user = 'tdi'
clickhouse_password = 'ANSKk08aPEDbFjDO'

# Create a ClickHouse client
client = Client(
    host=clickhouse_host,
    port=clickhouse_port,
    user=clickhouse_user,
    password=clickhouse_password,
    database=clickhouse_database
)

inputSatu = sys.argv[1]
# Inputan nama tabel didatabase
nm_tabel = inputSatu

# Example query: select all data from a table
query_master_rangka = 'SELECT * FROM master_rangka_koding'
query_koding = 'SELECT * FROM koding'
query_tarif = 'SELECT * FROM tarif'
query_wilayah = 'SELECT * FROM master_wilayah'
# Execute the query and fetch the result into a Pandas DataFrame
result_rangka = client.execute(query_master_rangka)
result_koding = client.execute(query_koding)
result_tarif = client.execute(query_tarif)
result_wilayah = client.execute(query_wilayah)

no_rangka = pd.DataFrame(result_rangka, columns=[desc[0] for desc in client.execute("DESCRIBE master_rangka_koding")])
koding = pd.DataFrame(result_koding, columns=[desc[0] for desc in client.execute("DESCRIBE koding")])
tarif = pd.DataFrame(result_tarif, columns=[desc[0] for desc in client.execute("DESCRIBE tarif")])
wilayah_df = pd.DataFrame(result_wilayah, columns=[desc[0] for desc in client.execute("DESCRIBE master_wilayah")])

# Example query: select all data from a table
query = f'SELECT * FROM data_sample_v2'
# Execute the query and fetch the result into a Pandas DataFrame
result_data = client.execute(query)

df = pd.DataFrame(result_data, columns=[desc[0] for desc in client.execute(f'DESCRIBE {nm_tabel}')])

# **ETL-2:Transform/Proses Data**

# Mengambil 8 Digit Norangka
df['no_rangka'] = df['no_rangka'].str.slice(0, 8)
df['no_rangka'] = df['no_rangka'].str.replace('*', '')

# Fungsi untuk membersihkan spasi tambahan di sebelah kiri dan kanan
def clean_whitespace(value):
    if isinstance(value, str):
        return value.strip()
    return value

# Menggunakan applymap() untuk menerapkan fungsi pada setiap elemen DataFrame
df = df.applymap(clean_whitespace)

# Menyiapkan df untuk data yang tidak ke mapping
no_mapping_plat = df.copy()

# Mengubah Kode Plat
# Mengganti nilai kode_plat 5 menjadi 4
# mengubah data yang kosong dengan nan
df = df.replace('', np.nan)
df.replace('NULL', np.nan, inplace=True)
df = df.dropna(subset=['kode_plat'])
df['kode_plat'] = df['kode_plat'].astype(int)
df['kode_plat'] = df['kode_plat'].replace(5, 4)

# Membuat kamus untuk memetakan kode plat ke deskripsi
kode_plat_to_deskripsi = {
    1: 'Hitam',
    2: 'Kuning',
    3: 'Putih',
    4: 'Merah'
}

# Menggunakan .map() untuk mengubah nilai kolom kode_plat
df['deskripsi_plat'] = df['kode_plat'].map(kode_plat_to_deskripsi)



# # Menghapus duplikat berdasarkan semua kolom
# df = df.drop_duplicates()

# Menampilkan baris-baris yang tidak ke mapping
no_mapping_plat = no_mapping_plat.loc[~no_mapping_plat.index.isin(df.index)]
no_mapping_plat = no_mapping_plat.fillna(0, axis=1)

# **ETL-3:Proses Validasi Kode Transaksi**

df_tarif = tarif[tarif['KD_TRANSAKSI'] == 8]

# Karena kode fungsi tidak diketahui maka akan mengambil persen terbesar
df_tarif.groupby(['KD_JENIS_2','KD_PLAT','PERSEN_PKB']).size().reset_index(name='Jumlah')

pkb = df_tarif.drop_duplicates(subset=['KD_JENIS_2', 'KD_PLAT', 'PERSEN_PKB'])

# Mengurutkan DataFrame berdasarkan kolom 'PERSEN_PKB' dalam urutan menurun
pkb = pkb.sort_values(by=['KD_JENIS_2','KD_PLAT','PERSEN_PKB'], ascending=False)

# Menghapus duplikat berdasarkan 'KD_JENIS' dan 'KD_PLAT', hanya menyimpan baris dengan nilai terbesar pada 'PERSEN_PKB'
pkb_result = pkb.drop_duplicates(subset=['KD_JENIS_2', 'KD_PLAT'], keep='first')

# **ETL-4:Proses Mapping No Rangka**

# Menambahkan indicator untuk mengetahui statusnya kegabung atau tidak ()
df_rangka = pd.merge(df, no_rangka, left_on='no_rangka', right_on='RANGKA', how='left', indicator=True)

# Menyaring baris yang hanya ada di DataFrame utama (df) atau yang tidak tergabung
no_mapping_rangka = df_rangka[df_rangka['_merge'] == 'left_only']
no_mapping_rangka = no_mapping_rangka.drop('_merge', axis=1)
no_mapping_rangka = no_mapping_rangka.fillna(0, axis=1)

no_mapping_rangka = no_mapping_rangka[['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
       'kode_golongan', 'kode_jenis_kendaraan',
       'kode_jenis_kendaraan_deskripsi', 'merk', 'model', 'tahun_pembuatan',
       'y0_tgl_mati_yad']]

df_rangka = df_rangka[df_rangka['_merge']=='both']
df_rangka = df_rangka[['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
       'kode_golongan','kode_jenis_kendaraan','kode_jenis_kendaraan_deskripsi','merk','model','tahun_pembuatan',
       'y0_tgl_mati_yad','KODING', 'TYPEKB',
       ]]

# **ETL-5:Identifikasi Koding dan Kode Jenis**

df_rangka.fillna(0, inplace=True)
df_rangka['tahun_pembuatan'] = df_rangka['tahun_pembuatan'].astype(int)

# Menambahkan indicator untuk mengetahui statusnya kegabung atau tidak ()
df_koding = pd.merge(df_rangka,koding, left_on=['KODING','tahun_pembuatan'], right_on=['KODING','TAHUN_BUAT'], how='left', indicator=True)

# Menyaring baris yang hanya ada di DataFrame utama (df) atau yang tidak tergabung
no_mapping_koding = df_koding[df_koding['_merge'] == 'left_only']
no_mapping_koding = no_mapping_koding.drop('_merge', axis=1)
no_mapping_koding = no_mapping_koding.fillna(0, axis=1)

no_mapping_koding = no_mapping_koding[['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
       'kode_golongan', 'kode_jenis_kendaraan',
       'kode_jenis_kendaraan_deskripsi', 'merk', 'model', 'tahun_pembuatan',
       'y0_tgl_mati_yad', 'KODING']]

# Pilih data yang ke mapping
df_koding = df_koding[df_koding['_merge']=='both']

# Menghapus kolom 'both'
df_koding = df_koding.drop('_merge', axis=1)

# **ETL-6:Perhitungan PKB**

# join papua dangan tarif
pkb = pd.merge(df_koding,pkb_result, left_on=['KD_JENIS_2','kode_plat'], right_on=['KD_JENIS_2','KD_PLAT'], how='inner')
pkb['PKB'] = pkb['DP_PKB'] * pkb['PERSEN_PKB'] / 100

# **ETL-7:Perhitungan PKB Progresif**

progresif = pkb
progresif['Tarif Progresif'] = 0
progresif['PKB PROGRESIF'] = progresif['Tarif Progresif'] * progresif['DP_PKB'] / 100

# **ETL-8:Status Tagihan**

total_pajak = progresif
total_pajak['y0_tgl_mati_yad'] = pd.to_datetime(total_pajak['y0_tgl_mati_yad'])
# Fungsi untuk menghitung status
def hitung_status(row):
    current_date = datetime.now().date()

    tgl_mati_yad_date = row['y0_tgl_mati_yad'].date()

    if tgl_mati_yad_date >= current_date:
        if tgl_mati_yad_date.year == current_date.year:
            return 'Potensi'
        elif tgl_mati_yad_date.year > current_date.year:
            return 'Realisasi'
    else:
        return 'Outstanding'

# Menambahkan kolom 'status' ke DataFrame
total_pajak['status'] = total_pajak.apply(hitung_status, axis=1)

# Mengonversi kolom datetime menjadi string dengan format tertentu
total_pajak['y0_tgl_mati_yad'] = total_pajak['y0_tgl_mati_yad'].dt.strftime('%Y-%m-%d')

# **Kode Wilayah**

# Fungsi untuk mengekstrak kode wilayah dari nomor polisi
def extract_kode_no_polisi(no_polisi):
    return no_polisi.split('-')[0]

# Menerapkan fungsi ke kolom no_polisi untuk mendapatkan kode wilayah
total_pajak['kode_no_polisi'] = total_pajak['no_polisi'].apply(extract_kode_no_polisi)

# Mengambil kolom yang diinginkan
kode_kendaraan = wilayah_df['kode_kendaraan']
kode_prov = wilayah_df['kode_prov']

# Membuat dictionary
kode_kendaraan_dict = dict(zip(kode_kendaraan, kode_prov))

total_pajak['kode_wilayah'] = total_pajak['kode_no_polisi'].map(kode_kendaraan_dict)
total_pajak.drop(columns=['kode_no_polisi'], inplace=True)

total_pajak['kode_wilayah'] = total_pajak['kode_wilayah'].astype(int)

# **ETL-9:Perhitungan Denda**

def hitung_denda(row):
    current_date = datetime.now().date()
    tgl_mati_yad_date = datetime.strptime(row['y0_tgl_mati_yad'], '%Y-%m-%d').date()
    tahun_mati = tgl_mati_yad_date.year
    selisih_tahun = current_date.year - tahun_mati

    if row['status'] == 'Outstanding' and selisih_tahun <= 5:
        return row['PKB'] * row['PERSEN_DENDA_PKB'] / 100 * selisih_tahun
    else:
        return 0

total_pajak['Denda_PKB'] = total_pajak.apply(hitung_denda, axis=1)

# **ETL-10:Perhitungan Total PKB**

total_pajak['Total Pajak'] = total_pajak['PKB'] + total_pajak['PKB PROGRESIF'] + total_pajak['Denda_PKB']

total_pajak = total_pajak.assign(KD_PLAT=total_pajak['kode_plat'])
# Menambahkan "_koding" ke belakang nama kolom yang disebutkan
total_pajak = total_pajak.rename(columns={
    'TYPE_KB': 'TYPE_KB_koding',
    'NJKB': 'NJKB_koding',
    'BOBOT': 'BOBOT_koding',
    'DP_PKB': 'DP_PKB_koding',
    'KD_JENIS': 'KD_JENIS_koding',
})
total_pajak = total_pajak[['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
       'kode_golongan', 'merk', 'kode_jenis_kendaraan_deskripsi',
       'y0_tgl_mati_yad', 'KODING', 'TAHUN_BUAT', 'TYPE_KB_koding',
       'NJKB_koding', 'BOBOT_koding', 'DP_PKB_koding', 'KD_JENIS_koding',
       'KD_JENIS_2', 'KD_PLAT', 'KD_FUNGSI', 'PERSEN_PKB', 'PERSEN_BBNKB',
       'PERSEN_DENDA_PKB', 'PERSEN_DENDA_BBNKB', 'KD_TRANSAKSI', 'PKB',
       'Tarif Progresif', 'PKB PROGRESIF','Denda_PKB','status','kode_wilayah', 'Total Pajak']]

total_pajak = total_pajak.fillna(0)

# **Insert Total Pajak Clickhouse**

# Assuming df is your DataFrame
total_pajak['no_mesin'] = total_pajak['no_mesin'].astype(str)
total_pajak['no_rangka'] = total_pajak['no_rangka'].astype(str)
total_pajak['no_polisi'] = total_pajak['no_polisi'].astype(str)
total_pajak['deskripsi_plat'] = total_pajak['deskripsi_plat'].astype(str)
total_pajak['kode_golongan'] = total_pajak['kode_golongan'].astype(str)
total_pajak['merk'] = total_pajak['merk'].astype(str)
total_pajak['kode_jenis_kendaraan_deskripsi'] = total_pajak['kode_jenis_kendaraan_deskripsi'].astype(str)
total_pajak['KODING'] = total_pajak['KODING'].astype(str)
total_pajak['KD_JENIS_2'] = total_pajak['KD_JENIS_2'].astype(str)
total_pajak['status'] = total_pajak['status'].astype(str)
total_pajak['kode_wilayah'] = total_pajak['kode_wilayah'].astype(str)

# Specify the ClickHouse table name
table_name = 'total_pajak_test_v2'

dtype_map = {
    'int64': 'Int64',
    'float64': 'Float64',
    'object': 'String'
}

# Create the table if it doesn't exist
if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
    create_table_query = f"CREATE TABLE {table_name} ("
    create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(total_pajak[col].dtype), "String")}' for col in total_pajak.columns)
    create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
    client.execute(create_table_query)

insert_query = f"INSERT INTO {table_name} VALUES"
client.insert_dataframe(insert_query, total_pajak, settings=dict(use_numpy=True))

# Close the ClickHouse connection
client.disconnect()

# Insert Data Tidak ke Mapping

# Fungsi untuk mengekstrak kode wilayah dari nomor polisi
def extract_kode_no_polisi(no_polisi):
    return no_polisi.split('-')[0]

# Menerapkan fungsi ke kolom no_polisi untuk mendapatkan kode wilayah
no_mapping_plat['kode_no_polisi'] = no_mapping_plat['no_polisi'].apply(extract_kode_no_polisi)

# Membuat dictionary
kode_kendaraan_dict = dict(zip(kode_kendaraan, kode_prov))

no_mapping_plat['kode_wilayah'] = no_mapping_plat['kode_no_polisi'].map(kode_kendaraan_dict)
no_mapping_plat.drop(columns=['kode_no_polisi'], inplace=True)

no_mapping_plat['kode_wilayah'] = no_mapping_plat['kode_wilayah'].astype(int)

# Assuming no_mapping_rangka is your DataFrame
columns_to_convert = ['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
                       'kode_golongan', 'kode_jenis_kendaraan', 'kode_jenis_kendaraan_deskripsi',
                       'merk', 'model', 'tahun_pembuatan', 'y0_tgl_mati_yad','kode_wilayah']

# Convert selected columns to string data type
no_mapping_plat[columns_to_convert] = no_mapping_plat[columns_to_convert].astype(str)

table_name = 'no_mapping_plat_v2'

dtype_map = {
    'int64': 'Int64',
    'float64': 'Float64',
    'object': 'String'}

# Create the table if it doesn't exist
if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
    create_table_query = f"CREATE TABLE {table_name} ("
    create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(no_mapping_plat[col].dtype), "String")}' for col in no_mapping_plat.columns)
    create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
    client.execute(create_table_query)

# Insert the data into ClickHouse
insert_query = f"INSERT INTO {table_name} VALUES"
client.insert_dataframe(insert_query, no_mapping_plat, settings=dict(use_numpy=True))

# Close the ClickHouse connection
client.disconnect()

# Menerapkan fungsi ke kolom no_polisi untuk mendapatkan kode wilayah
no_mapping_rangka['kode_no_polisi'] = no_mapping_rangka['no_polisi'].apply(extract_kode_no_polisi)

# Membuat dictionary
kode_kendaraan_dict = dict(zip(kode_kendaraan, kode_prov))

no_mapping_rangka['kode_wilayah'] = no_mapping_rangka['kode_no_polisi'].map(kode_kendaraan_dict)
no_mapping_rangka.drop(columns=['kode_no_polisi'], inplace=True)

no_mapping_rangka['kode_wilayah'] = no_mapping_rangka['kode_wilayah'].astype(int)

# Assuming no_mapping_rangka is your DataFrame
columns_to_convert = ['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
                       'kode_golongan', 'kode_jenis_kendaraan', 'kode_jenis_kendaraan_deskripsi',
                       'merk', 'model', 'tahun_pembuatan', 'y0_tgl_mati_yad','kode_wilayah']

# Convert selected columns to string data type
no_mapping_rangka[columns_to_convert] = no_mapping_rangka[columns_to_convert].astype(str)

table_name = 'no_mapping_rangka_v2'

dtype_map = {
    'int64': 'Int64',
    'float64': 'Float64',
    'object': 'String'}

# Create the table if it doesn't exist
if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
    create_table_query = f"CREATE TABLE {table_name} ("
    create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(no_mapping_rangka[col].dtype), "String")}' for col in no_mapping_rangka.columns)
    create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
    client.execute(create_table_query)

# Insert the data into ClickHouse
insert_query = f"INSERT INTO {table_name} VALUES"
client.insert_dataframe(insert_query, no_mapping_rangka, settings=dict(use_numpy=True))

# Close the ClickHouse connection
client.disconnect()

# Menerapkan fungsi ke kolom no_polisi untuk mendapatkan kode wilayah
no_mapping_koding['kode_no_polisi'] = no_mapping_koding['no_polisi'].apply(extract_kode_no_polisi)

# Membuat dictionary
kode_kendaraan_dict = dict(zip(kode_kendaraan, kode_prov))

no_mapping_koding['kode_wilayah'] = no_mapping_koding['kode_no_polisi'].map(kode_kendaraan_dict)
no_mapping_koding.drop(columns=['kode_no_polisi'], inplace=True)

no_mapping_koding['kode_wilayah'] = no_mapping_koding['kode_wilayah'].astype(int)

# Assuming no_mapping_koding is your DataFrame
columns_to_convert = ['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
       'kode_golongan', 'kode_jenis_kendaraan',
       'kode_jenis_kendaraan_deskripsi', 'merk', 'model', 'tahun_pembuatan',
       'y0_tgl_mati_yad', 'KODING','kode_wilayah']

# Convert selected columns to string data type
no_mapping_koding[columns_to_convert] = no_mapping_koding[columns_to_convert].astype(str)

table_name = 'no_mapping_koding_v2'

dtype_map = {
    'int64': 'Int64',
    'float64': 'Float64',
    'object': 'String'}

# Create the table if it doesn't exist
if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
    create_table_query = f"CREATE TABLE {table_name} ("
    create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(no_mapping_koding[col].dtype), "String")}' for col in no_mapping_koding.columns)
    create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
    client.execute(create_table_query)

# Insert the data into ClickHouse
insert_query = f"INSERT INTO {table_name} VALUES"
client.insert_dataframe(insert_query, no_mapping_koding, settings=dict(use_numpy=True))

# Close the ClickHouse connection
client.disconnect()