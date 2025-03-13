import re
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request as urlb
import py7zr
import os
from sklearn.model_selection import train_test_split
import pendulum
from airflow.decorators import (
    dag,
    task,
)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 2, 3, tz="UTC"),
    catchup=False,
    tags=["dag"],
)
def dag():

    @task()
    def GetData():

        url = "https://archive.org/download/stackexchange/english.stackexchange.com.7z"
        files = ["Users.xml", "Posts.xml"]

        if "data.7z" not in os.listdir("./data"):                                                   # Descarga de datos.
            urlb.urlretrieve(url, "./data/data.7z")

        archive = py7zr.SevenZipFile("./data/data.7z", 'r')                                         # Extracción de xml.
        for file in files:
            if file not in os.listdir("./data"):
                archive.extract(path = "./data", targets = [file])
                archive.reset()
        archive.close()

        for file in files:                                                                          # Conversión a pickle.
            if f"{file[:-4]}.pkl" not in os.listdir("./data"):
                print(f"{file[:-4]}.pkl no encontrado, procediendo a la compresión...")
                pd.read_xml(os.path.abspath(f"./data/{file}")).to_pickle(f"./data/{file[:-4]}.pkl")
                print(f"{file[:-4]}.pkl creado")

        
        Users = pd.read_pickle(os.path.abspath("./data/Users.pkl"))                                      # Lectura de datos.
        Posts = pd.read_pickle(os.path.abspath("./data/Posts.pkl"))

        Users_Train, Users_Test = train_test_split(Users,test_size=0.3)
        Posts_Train, Posts_Test = train_test_split(Posts,test_size=0.3)

        return Users_Train, Users_Test, Posts_Train, Posts_Test

    @task()
    def RemoveNull(UsersTrain):
        """
        Elimina las filas donde AccountId es nulo y elimina la columna AccountId del dataframe.
        Además de convertir el campo WebsiteUrl a un campo binario donde se indica si tiene o no una URL a una web.
        """        
        UsersTrain = UsersTrain["Users_Train"].dropna(subset=['AccountId'])
        UsersTrain = UsersTrain["Users_Train"].drop(columns=['AccountId'], errors='ignore')

        UsersTrain["WebsiteUrl"] = UsersTrain["WebsiteUrl"].notna()
        UsersTrain = UsersTrain.rename(columns={"WebsiteUrl":"HasUrl"})

        return Users_Train, Posts_Train

#--------------------------SACAR WEBSITEURL ---------

    @task()
    def ConvertDates(UsersTrain, Posts_Train):
        """
        Convierte las columnas que deberían ser fechas de string a datetime,
        omitiendo los milisegundos.
        """
        UsersTrain['CreationDate'] = pd.to_datetime(Users_Train['CreationDate']).astype('datetime64[s]')
        UsersTrain['LastAccessDate'] = pd.to_datetime(Users_Train['LastAccessDate']).astype('datetime64[s]')

        Posts_Train['CreationDate'] = pd.to_datetime(Posts_Train['CreationDate']).astype('datetime64[s]')
        Posts_Train['LastActivityDate'] = pd.to_datetime(Posts_Train['LastActivityDate']).astype('datetime64[s]')
        Posts_Train['LastEditDate'] = pd.to_datetime(Posts_Train['LastEditDate']).astype('datetime64[s]')
        Posts_Train['ClosedDate'] = pd.to_datetime(Posts_Train['ClosedDate']).astype('datetime64[s]')
        Posts_Train['CommunityOwnedDate'] = pd.to_datetime(Posts_Train['CommunityOwnedDate']).astype('datetime64[s]')

        return UsersTrain, Posts_Train

    @task
    def HTML_to_Text(UsersTrain, Posts_Train):   

        UsersTrain['AboutMe'] = UsersTrain['AboutMe'].apply
        (
        lambda muestra: BeautifulSoup(muestra, "html.parser").get_text().strip() if not (muestra is pd.NA) else pd.NA
        )
        UsersTrain["AboutMe"] = UsersTrain["AboutMe"].replace("",pd.NA)
        
        Posts_Train['Body'] = Posts_Train['Body'].apply
        (
        lambda muestra: BeautifulSoup(muestra, "html.parser").get_text().lower() if not (muestra is pd.NA) else pd.NA
        )
        Posts_Train['Body'] = Posts_Train['Body'].replace("",pd.NA)
        
        return UsersTrain, Posts_Train

    @task
    def Country_Location(UsersTrain):
	"""
	Carga formas equivalentes de escribir un país, para poder detectar la nacionalidad de un usuario.
	Modifica el campo "Location" para que sea únicamente el país. En caso de no reconocer ninguno de la lista, lo vuelve pd.NA.
	De igual forma también crea una nueva variable binaria llamada HasLocation que determina si tiene algún país en su campo Location o no.
	"""
	with open("../../data/paisesDict.json", "r", encoding="utf-8") as archivo:
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


        UsersTrain["Location"] = UsersTrain["Location"].apply(get_country)
        UsersTrain["HasLocation"] = UsersTrain["Location"].notna().astype(bool)

        return UsersTrain

    @task
    def Parse_Tags(Posts_Train):

        Posts_Train['Tags'] = Posts_Train['Tags'].fillna('')  # Rellenar valores NaN con cadenas vacías
        Posts_Train['Tags'] = Posts_Train['Tags'].apply(lambda x: x.split('|') if x else [])

        return Posts_Train



    Users_Train, Users_Test, Posts_Train, Posts_Test = GetData()
    UsersTrain, Posts_Train = RemoveNull(UsersTrain)
    UsersTrain, Posts_Train = ConvertDates(UsersTrain, Posts_Train)
    UsersTrain = HTML_to_Text(UsersTrain)
    Posts_Train = Parse_Tags(Posts_Train)
    UsersTrain = Country_Location(UsersTrain)
    
    

dag()
