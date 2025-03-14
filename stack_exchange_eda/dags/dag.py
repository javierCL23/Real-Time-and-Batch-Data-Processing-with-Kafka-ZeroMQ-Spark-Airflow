import re
import json
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

    @task(multiple_outputs=True)
    def GetData():
        url = "https://archive.org/download/stackexchange/english.stackexchange.com.7z"
        files = ["Users.xml", "Posts.xml"]
        pathToData = "data/raw/" 		#MOVERLO AL TOML
        if "Descargados" not in os.listdir(pathToData):                                                   # Descarga de datos.
            urlb.urlretrieve(url,str(pathToData)+"data.7z")
        
            archive = py7zr.SevenZipFile(str(pathToData)+"data.7z", 'r')                                         # Extracción de xml.
            for file in files:
                if file not in os.listdir(pathToData):
                    archive.extract(path = pathToData, targets = [file])
                    archive.reset()
            archive.close()

        for file in files:                                                                          # Conversión a pickle.
            if f"{file[:-4]}.pkl" not in os.listdir(pathToData):
                print(f"{file[:-4]}.pkl no encontrado, procediendo a la compresión...")
                pd.read_xml(os.path.abspath(str(pathToData)+str(file))).to_pickle(str(pathToData)+str(file[:-4])+".pkl")
                print(f"{file[:-4]}.pkl creado")


        Users = pd.read_pickle(os.path.abspath(str(pathToData)+"Users.pkl"))                                      # Lectura de datos.
        Posts = pd.read_pickle(os.path.abspath(str(pathToData)+"Posts.pkl"))

        Users_Train, Users_Test = train_test_split(Users,test_size=0.3)
        Posts_Train, Posts_Test = train_test_split(Posts,test_size=0.3)

        return {
            "Users": Users_Train,
            "Users_Test": Users_Test,
            "Posts": Posts_Train,
            "Posts_Test": Posts_Test,
        }
        
    @task()
    def RemoveNull(Users_Train):
        """
        Elimina las filas donde AccountId es nulo y elimina la columna AccountId del dataframe.
        Además de convertir el campo WebsiteUrl a un campo binario donde se indica si tiene o no una URL a una web.
        """        
        Users_Train = Users_Train.dropna(subset=['AccountId'])
        Users_Train = Users_Train.drop(columns=['AccountId'], errors='ignore')

        Users_Train["WebsiteUrl"] = Users_Train["WebsiteUrl"].notna()
        Users_Train = Users_Train.rename(columns={"WebsiteUrl":"HasUrl"})

        return Users_Train

#--------------------------SACAR WEBSITEURL ---------

    @task(multiple_outputs=True)
    def ConvertDates(Users_Train, Posts_Train):
        """
        Convierte las columnas que deberían ser fechas de string a datetime,
        omitiendo los milisegundos.
        """
        Users_Train['CreationDate'] = pd.to_datetime(Users_Train['CreationDate']).astype('datetime64[s]')
        Users_Train['LastAccessDate'] = pd.to_datetime(Users_Train['LastAccessDate']).astype('datetime64[s]')

        Posts_Train['CreationDate'] = pd.to_datetime(Posts_Train['CreationDate']).astype('datetime64[s]')
        Posts_Train['LastActivityDate'] = pd.to_datetime(Posts_Train['LastActivityDate']).astype('datetime64[s]')
        Posts_Train['LastEditDate'] = pd.to_datetime(Posts_Train['LastEditDate']).astype('datetime64[s]')
        Posts_Train['ClosedDate'] = pd.to_datetime(Posts_Train['ClosedDate']).astype('datetime64[s]')
        Posts_Train['CommunityOwnedDate'] = pd.to_datetime(Posts_Train['CommunityOwnedDate']).astype('datetime64[s]')

        return {"Users":Users_Train, "Posts":Posts_Train}


    @task(multiple_outputs=True)
    def HTML_to_Text(Users_Train, Posts_Train):   

        Users_Train['AboutMe'] = Users_Train['AboutMe'].apply
        (
        lambda muestra: BeautifulSoup(muestra, "html.parser").get_text().strip() if not (muestra is pd.NA) else pd.NA
        )
        Users_Train["AboutMe"] = Users_Train["AboutMe"].replace("",pd.NA)
    
        Posts_Train['Body'] = Posts_Train['Body'].apply
        (
        lambda muestra: BeautifulSoup(muestra, "html.parser").get_text().lower() if not (muestra is pd.NA) else pd.NA
        )
        Posts_Train['Body'] = Posts_Train['Body'].replace("",pd.NA)
        
        return {"Users":Users_Train, "Posts":Posts_Train}

    @task()
    def Country_Location(Users_Train):
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


        Users_Train["Location"] = Users_Train["Location"].apply(get_country)
        Users_Train["HasLocation"] = Users_Train["Location"].notna().astype(bool)

        return Users_Train

    @task()
    def Parse_Tags(Posts_Train):

        Posts_Train['Tags'] = Posts_Train['Tags'].fillna('')  # Rellenar valores NaN con cadenas vacías
        Posts_Train['Tags'] = Posts_Train['Tags'].apply(lambda x: x.split('|') if x else [])

        return Posts_Train



    data = GetData()
    Users_Train = data["Users"]
    Users_Test = data["Users_Test"]
    Posts_Train = data["Posts"]
    Posts_Test = data["Posts_Test"]

    Users_Train = RemoveNull(Users_Train)
    
    data = ConvertDates(Users_Train,Posts_Train)
    Users_Train = data["Users"]
    Posts_Train = data["Posts"]
    
    data = HTML_to_Text(Users_Train,Posts_Train)
    Users_Train = data["Users"]
    Posts_Train = data["Posts"]
    
    
    
    Posts_Train = Parse_Tags(Posts_Train)
    Users_Train = Country_Location(Users_Train)

dag()
