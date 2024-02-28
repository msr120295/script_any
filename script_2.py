# -*- coding: utf-8 -*-
"""script_2.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1bt1JyflGdi514FlN6fLsCOWmo5JPtRr4
"""

import pandas as pd
import numpy as np
from datetime import datetime

import polars as pl

from clickhouse_driver import Client
from clickhouse_driver import connect
import warnings


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

conn = connect('clickhouse://tdi:ANSKk08aPEDbFjDO@192.168.100.95:8547/jasaraharja')
cursor = conn.cursor()

# Matikan FutureWarning yang terkait dengan DataFrame.applymap
warnings.filterwarnings('ignore', category=FutureWarning, message="DataFrame.applymap has been deprecated*")

"""### === Read Data, Inisiasi Variabel==="""

# Query awal untuk membaca data
query = 'SELECT * FROM data_bali'

# Jumlah baris per batch
batch_size = 100000  # Misalnya, 1 juta baris per batch

# Inisialisasi indeks
offset = 0

column_names_query = f"DESCRIBE TABLE data_bali"
column_names_result = client.execute(column_names_query)
column_names = [desc[0] for desc in column_names_result]

#---------------------------------------------------
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


#----------------------------------------------------
# Inisiasi Output Table Name
table_name = '2_test_sample'
table_no_mapping_plat = '2_no_mapping_plat_test'
table_no_mapping_rangka = '2_no_mapping_rangka_test'
table_no_mapping_koding = '2_no_mapping_koding_test'

dtype_map = {
    'Int64': 'Int64',
    'Float64': 'Float64',
    'String': 'String'}

"""### === Validasi Kode Transaksi ==="""

df_tarif = tarif[tarif['KD_TRANSAKSI'] == 8]

# Karena kode fungsi tidak diketahui maka akan mengambil persen terbesar
df_tarif.groupby(['KD_JENIS_2','KD_PLAT','PERSEN_PKB']).size().reset_index(name='Jumlah')

pkb = df_tarif.drop_duplicates(subset=['KD_JENIS_2', 'KD_PLAT', 'PERSEN_PKB'])

# Mengurutkan DataFrame berdasarkan kolom 'PERSEN_PKB' dalam urutan menurun
pkb = pkb.sort_values(by=['KD_JENIS_2','KD_PLAT','PERSEN_PKB'], ascending=False)

# Menghapus duplikat berdasarkan 'KD_JENIS' dan 'KD_PLAT', hanya menyimpan baris dengan nilai terbesar pada 'PERSEN_PKB'
pkb_result = pkb.drop_duplicates(subset=['KD_JENIS_2', 'KD_PLAT'], keep='first')

"""### === Fungsi Proses Data ==="""

# Transform data ====================
def transform_data(df):
    # Membuat salinan DataFrame
    no_mapping_plat = df.copy()

    # Mengubah Kode Plat
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

    # Menyimpan baris-baris yang tidak memiliki mapping
    no_mapping_plat = no_mapping_plat.loc[~no_mapping_plat.index.isin(df.index)]
    no_mapping_plat = no_mapping_plat.fillna(0, axis=1)

    return df, no_mapping_plat

# Mapping no_rangka ====
def mapping_rangka(df,no_rangka):
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

    return df_rangka, no_mapping_rangka

# identifikasi koding dan kode_jenis =================
def koding_kdjenis(df_rangka,koding):
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

    return df_koding, no_mapping_koding

# Perhitungan PKB ======
def hitung_pkb(df_koding,pkb_result):
    pkb = pd.merge(df_koding,pkb_result, left_on=['KD_JENIS_2','kode_plat'], right_on=['KD_JENIS_2','KD_PLAT'], how='inner')
    pkb['PKB'] = pkb['DP_PKB'] * pkb['PERSEN_PKB'] / 100

    return pkb

# Perhitungan PKB Progresif =========
def hitung_progresif(pkb):
    progresif = pkb
    progresif['Tarif Progresif'] = 0
    progresif['PKB PROGRESIF'] = progresif['Tarif Progresif'] * progresif['DP_PKB'] / 100

    return progresif

# Kode Wilayah ===========
def kode_wilayah(total_pajak,wilayah_df):
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

    total_pajak['kode_wilayah'] = total_pajak['kode_wilayah'].fillna(0)
    total_pajak['kode_wilayah'] = total_pajak['kode_wilayah'].astype(int)
    return total_pajak


# Perhitungan Denda ===============
def hitung_denda(row):
    current_date = datetime.now().date()
    tgl_mati_yad_date = datetime.strptime(row['y0_tgl_mati_yad'], '%Y-%m-%d').date()
    tahun_mati = tgl_mati_yad_date.year
    selisih_tahun = current_date.year - tahun_mati

    if row['status'] == 'Outstanding' and selisih_tahun <= 5:
        return row['PKB'] * row['PERSEN_DENDA_PKB'] / 100 * selisih_tahun
    else:
        return 0

# Hitung Total PKB ==================
def hitung_total(total_pajak):
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
    return total_pajak


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

def clean_whitespace(value):
    if isinstance(value, str):
        return value.strip()
    return value

"""### === Fungsi Insert Data ==="""

# Fungsi extract no_polisi =======
def extract_kode_no_polisi(no_polisi):
    return no_polisi.split('-')[0]

# Insert Data =====
def insert_main_data(total_pajak, table_name, dtype_map):
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

    total_pajak = pl.from_pandas(total_pajak)

    # Create the table if it doesn't exist
    if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
        create_table_query = f"CREATE TABLE {table_name} ("
        create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(total_pajak[col].dtype), "String")}' for col in total_pajak.columns)
        create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
        client.execute(create_table_query)

    data_tuples = [tuple(row) for row in total_pajak.to_numpy()]

    # Insert data ke ClickHouse
    insert_query = f"INSERT INTO {table_name} VALUES"

    client.execute(insert_query, data_tuples)
    #cursor.executemany(insert_query, data_tuples)

# Insert no_mapping_plat =========
def insert_no_mapping_plat(no_mapping_plat,wilayah_df,dtype_map,table_name):
    # no_mapping_plat = kode_wilayah(no_mapping_plat,wilayah_df)
        # ========
    # Menerapkan fungsi ke kolom no_polisi untuk mendapatkan kode wilayah
    no_mapping_plat['kode_no_polisi'] = no_mapping_plat['no_polisi'].apply(extract_kode_no_polisi)

    # Mengambil kolom yang diinginkan
    kode_kendaraan = wilayah_df['kode_kendaraan']
    kode_prov = wilayah_df['kode_prov']

    # Membuat dictionary
    kode_kendaraan_dict = dict(zip(kode_kendaraan, kode_prov))

    ====
    no_mapping_plat['kode_wilayah'] = no_mapping_plat['kode_no_polisi'].map(kode_kendaraan_dict)
    no_mapping_plat.drop(columns=['kode_no_polisi'], inplace=True)

    # Assuming no_mapping_rangka is your DataFrame
    columns_to_convert = ['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
                           'kode_golongan', 'kode_jenis_kendaraan', 'kode_jenis_kendaraan_deskripsi',
                           'merk', 'model', 'tahun_pembuatan', 'y0_tgl_mati_yad','kode_wilayah']

    # Convert selected columns to string data type
    no_mapping_plat[columns_to_convert] = no_mapping_plat[columns_to_convert].astype(str)
    no_mapping_plat = pl.from_pandas(no_mapping_plat)

    if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
        create_table_query = f"CREATE TABLE {table_name} ("
        create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(no_mapping_plat[col].dtype), "String")}' for col in no_mapping_plat.columns)
        create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
        client.execute(create_table_query)

    data_tuples = [tuple(row) for row in no_mapping_plat.to_numpy()]

    # Insert data ke ClickHouse
    insert_query = f"INSERT INTO {table_name} VALUES"

    client.execute(insert_query, data_tuples)
    #cursor.executemany(insert_query, data_tuples)

# Insert no_mapping_rangka =========
def insert_no_mapping_rangka(no_mapping_rangka,wilayah_df,dtype_map,table_name):
    # no_mapping_rangka = kode_wilayah(no_mapping_rangka,wilayah_df)
        # =======
    # Menerapkan fungsi ke kolom no_polisi untuk mendapatkan kode wilayah
    no_mapping_rangka['kode_no_polisi'] = no_mapping_rangka['no_polisi'].apply(extract_kode_no_polisi)

    # Mengambil kolom yang diinginkan
    kode_kendaraan = wilayah_df['kode_kendaraan']
    kode_prov = wilayah_df['kode_prov']

    # Membuat dictionary
    kode_kendaraan_dict = dict(zip(kode_kendaraan, kode_prov))

    ===
    no_mapping_rangka['kode_wilayah'] = no_mapping_rangka['kode_no_polisi'].map(kode_kendaraan_dict)
    no_mapping_rangka.drop(columns=['kode_no_polisi'], inplace=True)

    # Assuming no_mapping_rangka is your DataFrame
    columns_to_convert = ['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
                       'kode_golongan', 'kode_jenis_kendaraan', 'kode_jenis_kendaraan_deskripsi',
                       'merk', 'model', 'tahun_pembuatan', 'y0_tgl_mati_yad','kode_wilayah']

    # Convert selected columns to string data type
    no_mapping_rangka[columns_to_convert] = no_mapping_rangka[columns_to_convert].astype(str)
    no_mapping_rangka = pl.from_pandas(no_mapping_rangka)

    if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
        create_table_query = f"CREATE TABLE {table_name} ("
        create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(no_mapping_rangka[col].dtype), "String")}' for col in no_mapping_rangka.columns)
        create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
        client.execute(create_table_query)

    data_tuples = [tuple(row) for row in no_mapping_rangka.to_numpy()]

    # Insert data ke ClickHouse
    insert_query = f"INSERT INTO {table_name} VALUES"

    client.execute(insert_query, data_tuples)
    #cursor.executemany(insert_query, data_tuples)

# Insert no_mapping_koding =========
def insert_no_mapping_koding(no_mapping_koding,wilayah_df,dtype_map,table_name):
    # no_mapping_koding = kode_wilayah(no_mapping_koding,wilayah_df)
        ========
    # Menerapkan fungsi ke kolom no_polisi untuk mendapatkan kode wilayah
    no_mapping_koding['kode_no_polisi'] = no_mapping_koding['no_polisi'].apply(extract_kode_no_polisi)

    # Mengambil kolom yang diinginkan
    kode_kendaraan = wilayah_df['kode_kendaraan']
    kode_prov = wilayah_df['kode_prov']

    # Membuat dictionary
    kode_kendaraan_dict = dict(zip(kode_kendaraan, kode_prov))
    ===
    no_mapping_koding['kode_wilayah'] = no_mapping_koding['kode_no_polisi'].map(kode_kendaraan_dict)
    no_mapping_koding.drop(columns=['kode_no_polisi'], inplace=True)

    # Assuming no_mapping_rangka is your DataFrame
    columns_to_convert = ['no_mesin', 'no_rangka', 'no_polisi', 'kode_plat', 'deskripsi_plat',
                           'kode_golongan', 'kode_jenis_kendaraan',
                           'kode_jenis_kendaraan_deskripsi', 'merk', 'model', 'tahun_pembuatan',
                           'y0_tgl_mati_yad', 'KODING','kode_wilayah']

    # Convert selected columns to string data type
    no_mapping_koding[columns_to_convert] = no_mapping_koding[columns_to_convert].astype(str)
    no_mapping_koding = pl.from_pandas(no_mapping_koding)

    if not client.execute(f"SHOW TABLES LIKE '{table_name}'"):
        create_table_query = f"CREATE TABLE {table_name} ("
        create_table_query += ', '.join(f'"{col}" {dtype_map.get(str(no_mapping_koding[col].dtype), "String")}' for col in no_mapping_koding.columns)
        create_table_query += f") ENGINE = MergeTree ORDER BY tuple();"
        client.execute(create_table_query)

    data_tuples = [tuple(row) for row in no_mapping_koding.to_numpy()]

    # Insert data ke ClickHouse
    insert_query = f"INSERT INTO {table_name} VALUES"

    client.execute(insert_query, data_tuples)
    #cursor.executemany(insert_query, data_tuples)

"""### === Main ==="""

# Iterasi untuk membaca data dalam batch
while True:
    # Membaca data dalam batch
    query_batch = f'{query} ORDER BY no_polisi ASC LIMIT {batch_size} OFFSET {offset}'
    result_data = client.execute(query_batch)
    if not result_data:  # Jika tidak ada data lagi, berhenti iterasi
        break

    # Simpan data dalam DataFrame
    df_batch = pl.DataFrame(result_data, schema=column_names)
    x = df_batch.to_pandas()

    # PROSES DATA
    # ===========================================================
    # -
    x['no_rangka'] = x['no_rangka'].str.slice(0, 8)
    x['no_rangka'] = x['no_rangka'].str.replace('*', '')
    x = x.applymap(clean_whitespace)
    df, no_mapping_plat = transform_data(x)
    df_rangka, no_mapping_rangka = mapping_rangka(df, no_rangka)
    df_koding, no_mapping_koding = koding_kdjenis(df_rangka,koding)

    pkb = hitung_pkb(df_koding,pkb_result)
    progresif = hitung_progresif(pkb)

    # Status ===
    total_pajak = progresif
    total_pajak['y0_tgl_mati_yad'] = pd.to_datetime(total_pajak['y0_tgl_mati_yad'])
    total_pajak['status'] = total_pajak.apply(hitung_status, axis=1)
    total_pajak['y0_tgl_mati_yad'] = total_pajak['y0_tgl_mati_yad'].dt.strftime('%Y-%m-%d')

    # # =====
    total_pajak = kode_wilayah(total_pajak,wilayah_df)
    total_pajak['Denda_PKB'] = total_pajak.apply(hitung_denda, axis=1)
    total_pajak = hitung_total(total_pajak)
    # # =========================================================

    # # INSERT DATA
    # # ===========================================================
    insert_main_data(total_pajak, table_name, dtype_map)
    insert_no_mapping_plat(no_mapping_plat,wilayah_df,dtype_map,table_no_mapping_plat)
    insert_no_mapping_rangka(no_mapping_rangka,wilayah_df,dtype_map,table_no_mapping_rangka)
    insert_no_mapping_koding(no_mapping_koding,wilayah_df,dtype_map,table_no_mapping_koding)
    # -
    # ===========================================================

    # Persiapkan untuk batch berikutnya
    # offset += batch_size
    offset = offset + batch_size

