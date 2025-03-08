import pandas as pd
import os

for file in os.listdir():
	if file[-3:] == "xml":
		pd.read_xml(f"./{file}").to_pickle(f"./{file[:-4]}.pkl")
		print(f"{file[:-4]}.pkl creado.")

print("¿Todavía estás ahí? Esto ya ha terminado.")