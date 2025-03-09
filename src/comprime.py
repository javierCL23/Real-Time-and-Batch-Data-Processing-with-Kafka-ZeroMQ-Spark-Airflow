import pandas as pd
import os

"""
USO: se tiene que ejecutar en la misma carpeta en la que se tienen los archivos xml.
Los archivos originales .xml no se destruyen, se conservan.
Para leer los archivos .pkl resultantes se debe usar $pd.read_pickle("Dir")
"""

#!!!NOTA!!! Únicamente tiene que estar los archivos a comprimir

for file in os.listdir():
	if file[-3:] == "xml":
		pd.read_xml(f"./{file}").to_pickle(f"./{file[:-4]}.pkl")
		print(f"{file[:-4]}.pkl creado.")

print("¿Todavía estás ahí? Esto ya ha terminado.")
