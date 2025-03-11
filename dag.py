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
        files = ["Users.xml", "Posts.pkl"]

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

        
        Users = pd.read_pickle(os.abspath("./data/Users.pkl"))                                      # Lectura de datos.
        Posts = pd.read_pickle(os.abspath("./data/Posts.pkl"))

        Users_Train, Users_Test = train_test_split(Users,test_size=0.3)
        Posts_Train, Posts_Test = train_test_split(Posts,test_size=0.3)

        return Users_Train, Users_Test, Posts_Train, Posts_Test

    @task()
    def RemoveNull(UsersTrain):
        
        UsersTrain = UsersTrain["Users_Train"].dropna(subset=['AccountId'])
        UsersTrain = UsersTrain["Users_Train"].drop(columns=['AccountId'], errors='ignore')

        UsersTrain["WebsiteUrl"] = UsersTrain["WebsiteUrl"].notna()
        UsersTrain = UsersTrain.rename(columns={"WebsiteUrl":"HasUrl"})

        return Users_Train, Posts_Train

    @task()
    def ConvertDates(UsersTrain, Posts_Train):

        UsersTrain['CreationDate'] = pd.to_datetime(data["Users_Train"]['CreationDate']).astype('datetime64[s]')
        UsersTrain['LastAccessDate'] = pd.to_datetime(data["Users_Train"]['LastAccessDate']).astype('datetime64[s]')

        Posts_Train['CreationDate'] = pd.to_datetime(data["Posts_Train"]['CreationDate']).astype('datetime64[s]')
        Posts_Train['LastActivityDate'] = pd.to_datetime(data["Posts_Train"]['LastActivityDate']).astype('datetime64[s]')
        Posts_Train['LastEditDate'] = pd.to_datetime(data["Posts_Train"]['LastEditDate']).astype('datetime64[s]')
        Posts_Train['ClosedDate'] = pd.to_datetime(data["Posts_Train"]['ClosedDate']).astype('datetime64[s]')
        Posts_Train['CommunityOwnedDate'] = pd.to_datetime(data["Posts_Train"]['CommunityOwnedDate']).astype('datetime64[s]')

        return UsersTrain, Posts_Train

    @task
    def HTML_to_Text(UsersTrain, Posts_Train):   

        UsersTrain['AboutMe'] = UsersTrain['AboutMe'].apply
        (
        lambda muestra: BeautifulSoup(muestra, "html.parser").get_text().strip() if pd.notna(muestra) else ""
        )

        Posts_Train['Body'] = Posts_Train['Body'].apply
        (
        lambda muestra: BeautifulSoup(muestra, "html.parser").get_text().lower() if pd.notna(muestra) else ""
        )

        return UsersTrain, Posts_Train

    @task
    def Country_Location(UsersTrain):
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
    
    




countries = {
    "Afghanistan": ["Afghanistan", "afghanistan", "افغانستان", "AF"],
    "Albania": ["Albania", "albania", "Shqipëri", "shqipëri", "AL"],
    "Germany": ["Germany", "germany", "Deutschland", "deutschland", "DE"],
    "Spain": ["Spain", "spain", "España", "españa", "ES"],
    "United States": ["United States", "united states", "USA", "us"],
    "United Kingdom": ["United Kingdom", "united kingdom", "UK", "uk", "Britain", "britain"],
    "Brazil": ["Brazil", "brazil", "Brasil", "brasil", "BR"],
    "France": ["France", "france", "FR"],
    "Italy": ["Italy", "italy", "Italia", "italia", "IT"],
    "Canada": ["Canada", "canada", "CA"],
    "Australia": ["Australia", "australia", "AU"],
    "Algeria": ["Algeria", "algeria", "الجزائر", "DZ"],
    "Argentina": ["Argentina", "argentina", "AR"],
    "Armenia": ["Armenia", "armenia", "Հայաստան", "AM"],
    "Australia": ["Australia", "australia", "AU"],
    "Austria": ["Austria", "austria", "Österreich", "AT"],
    "Azerbaijan": ["Azerbaijan", "azerbaijan", "Azərbaycan", "AZ"],
    "Bahamas": ["Bahamas", "bahamas", "BS"],
    "Bangladesh": ["Bangladesh", "bangladesh", "বাংলাদেশ", "BD"],
    "Barbados": ["Barbados", "barbados", "BB"],
    "Bahrain": ["Bahrain", "bahrain", "البحرين", "BH"],
    "Belgium": ["Belgium", "belgium", "BEL"],
    "Belize": ["Belize", "belize", "BZ"],
    "Benin": ["Benin", "benin", "BJ"],
    "Belarus": ["Belarus", "belarus", "Беларусь", "BY"],
    "Burma": ["Burma", "burma", "Myanmar", "MM"],
    "Bolivia": ["Bolivia", "bolivia", "BO"],
    "Bosnia and Herzegovina": ["Bosnia and Herzegovina", "bosnia and herzegovina", "Bosna i Hercegovina", "BA"],
    "Botswana": ["Botswana", "botswana", "BW"],
    "Brazil": ["Brazil", "brazil", "Brasil", "BR"],
    "Brunei": ["Brunei", "brunei", "BN"],
    "Bulgaria": ["Bulgaria", "bulgaria", "България", "BG"],
    "Burkina Faso": ["Burkina Faso", "burkina faso", "BF"],
    "Burundi": ["Burundi", "burundi", "BI"],
    "Bhutan": ["Bhutan", "bhutan", "འབྲུག", "BT"],
    "Cabo Verde": ["Cabo Verde", "cabo verde", "CV"],
    "Cambodia": ["Cambodia", "cambodia", "កម្ពុជា", "KH"],
    "Cameroon": ["Cameroon", "cameroon", "CM"],
    "Canada": ["Canada", "canada", "CA"],
    "Qatar": ["Qatar", "qatar", "QA"],
    "Chile": ["Chile", "chile", "CL"],
    "China": ["China", "china", "中国", "CN"],
    "Cyprus": ["Cyprus", "cyprus", "Κύπρος", "CY"],
    "Colombia": ["Colombia", "colombia", "CO"],
    "Comoros": ["Comoros", "comoros", "KM"],
    "North Korea": ["North Korea", "north korea", "북한", "KP"],
    "South Korea": ["South Korea", "south korea", "대한민국", "KR"],
    "Ivory Coast": ["Ivory Coast", "ivory coast", "Côte d'Ivoire", "CI"],
    "Costa Rica": ["Costa Rica", "costa rica", "CR"],
    "Croatia": ["Croatia", "croatia", "Hrvatska", "HR"],
    "Cuba": ["Cuba", "cuba", "CU"],
    "Denmark": ["Denmark", "denmark", "DK"],
    "Dominica": ["Dominica", "dominica", "DM"],
    "Ecuador": ["Ecuador", "ecuador", "EC"],
    "Egypt": ["Egypt", "egypt", "مصر", "EG"],
    "El Salvador": ["El Salvador", "el salvador", "SV"],
    "United Arab Emirates": ["United Arab Emirates", "united arab emirates", "الإمارات العربية المتحدة", "AE"],
    "Eritrea": ["Eritrea", "eritrea", "ER"],
    "Slovakia": ["Slovakia", "slovakia", "Slovensko", "SK"],
    "Slovenia": ["Slovenia", "slovenia", "Slovenija", "SI"],
    "Estonia": ["Estonia", "estonia", "EE"],
    "Ethiopia": ["Ethiopia", "ethiopia", "ኢትዮጵያ", "ET"],
    "Philippines": ["Philippines", "philippines", "フィリピン", "PH"],
    "Finland": ["Finland", "finland", "Suomi", "FI"],
    "Fiji": ["Fiji", "fiji", "FJ"],
    "France": ["France", "france", "FR"],
    "Gabon": ["Gabon", "gabon", "GA"],
    "Gambia": ["Gambia", "gambia", "GM"],
    "Georgia": ["Georgia", "georgia", "საქართველო", "GE"],
    "Ghana": ["Ghana", "ghana", "GH"],
    "Grenada": ["Grenada", "grenada", "GD"],
    "Greece": ["Greece", "greece", "Ελλάδα", "GR"],
    "Guatemala": ["Guatemala", "guatemala", "GT"],
    "Guyana": ["Guyana", "guyana", "GY"],
    "Guinea": ["Guinea", "guinea", "GN"],
    "Guinea-Bissau": ["Guinea-Bissau", "guinea-bissau", "GW"],
    "Haiti": ["Haiti", "haiti", "HT"],
    "Honduras": ["Honduras", "honduras", "HN"],
    "Hungary": ["Hungary", "hungary", "Magyarország", "HU"],
    "India": ["India", "india", "भारत", "IN"],
    "Indonesia": ["Indonesia", "indonesia", "Indonesia", "ID"],
    "Iraq": ["Iraq", "iraq", "العراق", "IQ"],
    "Iran": ["Iran", "iran", "ایران", "IR"],
    "Ireland": ["Ireland", "ireland", "Éire", "IE"],
    "Iceland": ["Iceland", "iceland", "Ísland", "IS"],
    "Marshall Islands": ["Marshall Islands", "marshall islands", "MH"],
    "Solomon Islands": ["Solomon Islands", "solomon islands", "SB"],
    "Israel": ["Israel", "israel", "ישראל", "IL"],
    "Italy": ["Italy", "italy", "Italia", "IT"],
    "Jamaica": ["Jamaica", "jamaica", "JM"],
    "Japan": ["Japan", "japan", "日本", "JP"],
    "Jordan": ["Jordan", "jordan", "الأردن", "JO"],
    "Kazakhstan": ["Kazakhstan", "kazakhstan", "Қазақстан", "KZ"],
    "Kenya": ["Kenya", "kenya", "KE"],
    "Kyrgyzstan": ["Kyrgyzstan", "kyrgyzstan", "Кыргызстан", "KG"],
    "Kiribati": ["Kiribati", "kiribati", "KI"],
    "Kuwait": ["Kuwait", "kuwait", "KW"],
    "Laos": ["Laos", "laos", "ລາວ", "LA"],
    "Lesotho": ["Lesotho", "lesotho", "LS"],
    "Latvia": ["Latvia", "latvia", "Latvija", "LV"],
    "Lebanon": ["Lebanon", "lebanon", "لبنان", "LB"],
    "Liberia": ["Liberia", "liberia", "LR"],
    "Libya": ["Libya", "libya", "ليبيا", "LY"],
    "Liechtenstein": ["Liechtenstein", "liechtenstein", "LI"],
    "Lithuania": ["Lithuania", "lithuania", "Lietuva", "LT"],
    "Luxembourg": ["Luxembourg", "luxembourg", "LU"],
    "Madagascar": ["Madagascar", "madagascar", "MG"],
    "Malaysia": ["Malaysia", "malaysia", "Malaysia", "MY"],
    "Malawi": ["Malawi", "malawi", "MW"],
    "Maldives": ["Maldives", "maldives", "MV"],
    "Mali": ["Mali", "mali", "ML"],
    "Malta": ["Malta", "malta", "MT"],
    "Morocco": ["Morocco", "morocco", "المغرب", "MA"],
    "Mauritius": ["Mauritius", "mauritius", "MU"],
    "Mauritania": ["Mauritania", "mauritania", "MR"],
    "Mexico": ["Mexico", "mexico", "México", "MX"],
    "Micronesia": ["Micronesia", "micronesia", "FM"],
    "Moldova": ["Moldova", "moldova", "Moldova", "MD"],
    "Monaco": ["Monaco", "monaco", "MC"],
    "Mongolia": ["Mongolia", "mongolia", "Монгол Улс", "MN"],
    "Montenegro": ["Montenegro", "montenegro", "CG"],
    "Mozambique": ["Mozambique", "mozambique", "MZ"],
    "Namibia": ["Namibia", "namibia", "NA"],
    "Nauru": ["Nauru", "nauru", "NR"],
    "Nepal": ["Nepal", "nepal", "नेपाल", "NP"],
    "Nicaragua": ["Nicaragua", "nicaragua", "NI"],
    "Niger": ["Niger", "niger", "NE"],
    "Nigeria": ["Nigeria", "nigeria", "NG"],
    "Norway": ["Norway", "norway", "NO"],
    "New Zealand": ["New Zealand", "new zealand", "Aotearoa", "NZ"],
    "Oman": ["Oman", "oman", "OM"],
    "Netherlands": ["Netherlands", "netherlands", "Nederland", "NL"],
    "Pakistan": ["Pakistan", "pakistan", "پاکستان", "PK"],
    "Palau": ["Palau", "palau", "PW"],
    "Panama": ["Panama", "panama", "PA"],
    "Papua New Guinea": ["Papua New Guinea", "papua new guinea", "PG"],
    "Paraguay": ["Paraguay", "paraguay", "PY"],
    "Peru": ["Peru", "peru", "Perú", "PE"],
    "Poland": ["Poland", "poland", "Polska", "PL"],
    "Portugal": ["Portugal", "portugal", "PT"],
    "Romania": ["Romania", "romania", "România", "RO"],
    "Russia": ["Russia", "russia", "Россия", "RU"],
    "Rwanda": ["Rwanda", "rwanda", "RW"],
    "Saint Kitts and Nevis": ["Saint Kitts and Nevis", "saint kitts and nevis", "KN"],
    "Saint Lucia": ["Saint Lucia", "saint lucia", "LC"],
    "Saint Vincent and the Grenadines": ["Saint Vincent and the Grenadines", "saint vincent and the grenadines", "VC"],
    "Samoa": ["Samoa", "samoa", "WS"],
    "San Marino": ["San Marino", "san marino", "SM"],
    "Sao Tome and Principe": ["Sao Tome and Principe", "sao tome and principe", "ST"],
    "Senegal": ["Senegal", "senegal", "SN"],
    "Serbia": ["Serbia", "serbia", "RS"],
    "Seychelles": ["Seychelles", "seychelles", "SC"],
    "Sierra Leone": ["Sierra Leone", "sierra leone", "SL"],
    "Singapore": ["Singapore", "singapore", "SG"],
    "Syria": ["Syria", "syria", "سوريا", "SY"],
    "Somalia": ["Somalia", "somalia", "SO"],
    "Sri Lanka": ["Sri Lanka", "sri lanka", "ශ්‍රී ලංකා", "LK"],
    "Swaziland": ["Swaziland", "swaziland", "SZ"],
    "Sudan": ["Sudan", "sudan", "SD"],
    "South Sudan": ["South Sudan", "south sudan", "SS"],
    "Sweden": ["Sweden", "sweden", "SE"],
    "Switzerland": ["Switzerland", "switzerland", "Schweiz", "CH"],
    "Suriname": ["Suriname", "suriname", "SR"],
    "Thailand": ["Thailand", "thailand", "ไทย", "TH"],
    "Tanzania": ["Tanzania", "tanzania", "TZ"],
    "Tajikistan": ["Tajikistan", "tajikistan", "Таджикистан", "TJ"],
    "Timor-Leste": ["Timor-Leste", "timor-leste", "TL"],
    "Togo": ["Togo", "togo", "TG"],
    "Tonga": ["Tonga", "tonga", "TO"],
    "Trinidad and Tobago": ["Trinidad and Tobago", "trinidad and tobago", "TT"],
    "Tunisia": ["Tunisia", "tunisia", "تونس", "TN"],
    "Turkmenistan": ["Turkmenistan", "turkmenistan", "Türkmenistan", "TM"],
    "Turkey": ["Turkey", "turkey", "Türkiye", "TR"],
    "Tuvalu": ["Tuvalu", "tuvalu", "TV"],
    "Ukraine": ["Ukraine", "ukraine", "Україна", "UA"],
    "Uganda": ["Uganda", "uganda", "UG"],
    "Uruguay": ["Uruguay", "uruguay", "UY"],
    "Uzbekistan": ["Uzbekistan", "uzbekistan", "Ўзбекистон", "UZ"],
    "Vanuatu": ["Vanuatu", "vanuatu", "VU"],
    "Venezuela": ["Venezuela", "venezuela", "VE"],
    "Vietnam": ["Vietnam", "vietnam", "Việt Nam", "VN"],
    "Yemen": ["Yemen", "yemen", "اليمن", "YE"],
    "Yemen": ["Yemen", "yemen", "YE"],
    "Zambia": ["Zambia", "zambia", "ZM"],
    "Zimbabwe": ["Zimbabwe", "zimbabwe", "ZW"]
}
dag()