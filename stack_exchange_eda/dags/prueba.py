import toml
import os

# Obtener la ruta del directorio donde está el script actual
current_dir = os.path.dirname(os.path.abspath(__file__))

# Obtener la ruta del archivo TOML en el directorio superior (abuelo)
toml_file_path = os.path.join(current_dir, '..', '..', 'pyproject.toml')

# Normalizar la ruta para asegurarse de que se resuelva correctamente
toml_file_path = os.path.normpath(toml_file_path)

# Cargar el archivo TOML
config = toml.load(toml_file_path)

# Acceder a las variables definidas en el TOML
DB_PATH = config['project']['paths']['db_path']
pathToData = config['project']['paths']['pathToData']
countries_dict_path = config['project']['paths']['countries_dict_path']
processed_data_path = config['project']['paths']['processed_data_path']

print(DB_PATH, pathToData, countries_dict_path, processed_data_path)
