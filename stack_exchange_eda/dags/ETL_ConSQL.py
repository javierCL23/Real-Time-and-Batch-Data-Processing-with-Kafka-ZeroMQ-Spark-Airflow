import re
import json
import os

from bs4 import BeautifulSoup
import pandas as pd
import urllib.request as urlb
import py7zr
import sqlite3
import plotly.express as px
import plotly
from sklearn.model_selection import train_test_split
import pendulum
from airflow.decorators import dag, task
import toml

config = toml.load("pyproject.toml")

DB_PATH = config['tools']['project_paths']['db_path']
pathToData = config['tools']['project_paths']['pathToData']
countries_dict_path = config['tools']['project_paths']['countries_dict_path']
processed_data_path = config['tools']['project_paths']['processed_data_path']

def XML_to_pkl(ruta_archivos,file):        
    # Verifica si la ruta existe
    if not os.path.isdir(ruta_archivos):
        print(f"La ruta especificada no existe: {ruta_archivos}")
    else:
        #Comprueba si es un .xml
        if file.lower().endswith(".xml"):
            ruta_completa = os.path.join(ruta_archivos, file)
            salida_pkl = os.path.join(ruta_archivos, f"{file[:-4]}.pkl")
            #Convierte a pkl
            pd.read_xml(ruta_completa).to_pickle(salida_pkl)
            print(f"{file[:-4]}.pkl creado.")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 2, 3, tz="UTC"),
    catchup=False,
    tags=["dag","ETL"],
)
def etl_dag():

    @task(multiple_outputs=True)
    def GetData():
        """
        Esta función únicamente se ejecuta entera una vez o en caso de que se borren los datos. 
        Se encarga de descargar, descomprimir y pasar a pickle los datos formato xml.
        También crea un db en DB_PATH que se usará para el manejo de los dataframes.
        Devuelve el nombre de los conjuntos que se usarán separados en train y test.
        """
        url = "https://archive.org/download/stackexchange/english.stackexchange.com.7z"
        files = ["Users.xml", "Posts.xml"]
        archive = 0
        
        for file in files:
            if file not in os.listdir(pathToData):
                if "data.7z" not in os.listdir(pathToData):
                    urlb.urlretrieve(url, str(pathToData) + "data.7z")
                archive = py7zr.SevenZipFile(str(pathToData) + "data.7z", 'r')
                archive.extract(path=pathToData, targets=[file])
                archive.reset()
                XML_to_pkl(ruta_archivos=pathToData,file=file)
        if archive != 0:
            archive.close()
        
        Users = pd.read_pickle(os.path.abspath(str(pathToData) + "Users.pkl"))
        Posts = pd.read_pickle(os.path.abspath(str(pathToData) + "Posts.pkl"))

        Users_Train, Users_Test = train_test_split(Users, test_size=0.3)
        Posts_Train, Posts_Test = train_test_split(Posts, test_size=0.3)

        conn = sqlite3.connect(DB_PATH)
        Users_Train.to_sql("Users_Train", conn, if_exists="replace", index=False)
        Users_Test.to_sql("Users_Test", conn, if_exists="replace", index=False)
        Posts_Train.to_sql("Posts_Train", conn, if_exists="replace", index=False)
        Posts_Test.to_sql("Posts_Test", conn, if_exists="replace", index=False)
        conn.close()

        return {
            "Users_Train": "Users_Train",
            "Users_Test": "Users_Test",
            "Posts_Train": "Posts_Train",
            "Posts_Test": "Posts_Test"
        }

    @task()
    def RemoveNull(table_name: str):
        """
        Elimina las filas donde AccountId es nulo y elimina la columna AccountId del dataframe Users.
        También convierte todos los tipos de Null a uno único: pd.NA (O eso se supone puesto que en la práctica por alguna razón no pasa)
        """        
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df = df.dropna(subset=['AccountId'])
        df = df.drop(columns=['AccountId'], errors='ignore')
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        return table_name

    @task()
    def TransformWebsiteURL(table_name: str):   #TICK
        """
        Modifica el campo WebsiteUrl del dataframe de Users a un campo binario que marca si tiene alguna URL en su perfil o no
        """
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df["WebsiteUrl"] = df["WebsiteUrl"].notna()
        df = df.rename(columns={"WebsiteUrl": "HasUrl"})
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        return table_name

    @task()
    def ConvertDates(table_name: str):          #TICK
        """
        Convierte las columnas que deberían ser fechas de string a datetime,
        omitiendo los milisegundos.
        """
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        if "Users" in table_name:
            campos = ['CreationDate', 'LastAccessDate']
        else:
            campos = ['CreationDate','LastActivityDate', 'LastEditDate', 'ClosedDate', 'CommunityOwnedDate']
        
        for col in campos:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).astype('datetime64[s]')
                
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        return table_name

    @task()
    def HTML_to_Text(table_name: str):          #TICK
        """
        Convierte las variables que contienen HTML a un string donde solo se tiene el texto contenido en ese HTML
        """
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        if 'Users' in table_name:
            df['AboutMe'] = df['AboutMe'].apply(lambda x: BeautifulSoup(x, "html.parser").get_text().strip() if isinstance(x, str) and x.strip() else pd.NA)
            df["AboutMe"] = df["AboutMe"].replace("", pd.NA)
        if 'Posts' in table_name:
            df['Body'] = df['Body'].apply(lambda x: BeautifulSoup(x, "html.parser").get_text().lower() if isinstance(x, str) and x.strip() else pd.NA)
            df['Body'] = df['Body'].replace("", pd.NA)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        return table_name

    @task()
    def Country_Location(table_name:str):
        """
        Carga formas equivalentes de escribir un país, para poder detectar la nacionalidad de un usuario.
        Modifica el campo "Location" para que sea únicamente el país. En caso de no reconocer ninguno de la lista, lo vuelve pd.NA.
        De igual forma también crea una nueva variable binaria llamada HasLocation que determina si tiene algún país en su campo Location o no.
        """
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        
        with open(countries_dict_path, "r", encoding="utf-8") as archivo:
            countries = json.load(archivo)

        pattern_dict = {}
        for country, variations in countries.items():
            safe_variations = [re.escape(v) for v in variations]  # Escapar caracteres especiales
            pattern = r'\b(' + '|'.join(safe_variations) + r')\b'  # \b asegura coincidencia exacta
            pattern_dict[country] = re.compile(pattern, re.IGNORECASE)

            def get_country(location):
                if not isinstance(location, str):
                    return pd.NA  # Si no es una cadena, devolvemos NA

                for country, pattern in pattern_dict.items():
                    if pattern.search(location):
                        return country  # Retorna el primer país coincidente

                return pd.NA  # Si no hay coincidencia


        df["Location"] = df["Location"].apply(get_country)
        df["HasLocation"] = df["Location"].notna().astype(bool)
        
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        return table_name

    @task()
    def Parse_Tags(table_name):
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        
        df['Tags'] = df['Tags'].fillna('')  # Rellenar valores NaN con cadenas vacías
        df['Tags'] = df['Tags'].apply(lambda x: x.split('|') if x else [])

        df.to_sql(table_name,conn,if_exists='replace',index = False)
        conn.close()
        return table_name


    @task()
    def LoadData(table_name: str):
        """
        Carga todos los cambios en unos nuevos json (uno por conjunto) donde tienen todos los cambios aplicados
        """
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        # Guardar el DataFrame en un archivo CSV
        csv_path = os.path.join(processed_data_path, f"{table_name}.csv")
        df.to_csv(csv_path, index=False)
        print(f"DataFrame guardado en {csv_path}")
    
    @task()
    def LoadGraph(table_name: str):
        """
        Genera un gráfico interactivo del número de usuarios por país y lo guarda en formato html.
        """
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT Location FROM {table_name}", conn)

        
        counts = df.value_counts()
        countriesDf = pd.DataFrame(counts).reset_index()
        countriesDf.columns = ["country","count"]

        fig = px.choropleth(countriesDf, locations="country",
                            color="count",
                            hover_name="country", 
                            locationmode = "country names",
                            color_continuous_scale=px.colors.sequential.OrRd)
        plotly.offline.plot(fig, filename=os.path.join(processed_data_path, "users_per_country.html"))
        conn.close()
     
    #-------------------------------------------------------------------------------------------------------------------
    # Obtener los nombres de las tablas
    data = GetData()
    users_train_table = data["Users_Train"]
    posts_train_table = data["Posts_Train"]

    # Procesar Users_Train
    users_train_table = RemoveNull(table_name=users_train_table)
    users_train_table = TransformWebsiteURL(table_name=users_train_table)
    users_train_table = ConvertDates(table_name=users_train_table)
    users_train_table = HTML_to_Text(table_name=users_train_table)
    users_train_table = Country_Location(table_name=users_train_table)
    LoadData(table_name=users_train_table)

    # Procesar Posts_Train
    posts_train_table = ConvertDates(table_name=posts_train_table)
    posts_train_table = HTML_to_Text(table_name=posts_train_table)
    #posts_train_table = Parse_Tags(table_name=posts_train_table)
    
    # Guardado de datos
    LoadData(table_name=posts_train_table)
    LoadGraph(table_name=users_train_table)

etl_dag()