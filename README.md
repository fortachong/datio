# datio
Reto Pyme hackathon BBVA 2022

## datio_scraper

Módulo de scrapping de sitio de información geográfica DENUE. Se ejecuta el siguiente pipeline:

01_limpiador.py -> 02_codificador.py -> 03_info.py -> 04_limpiador_info.py -> 05_google.py -> 06_consolidador.py

### Ejecución

#### Limpieza de archivo
```code
python 01_limpiador.py data/entrada/Final_Data_Hackathon_2022.csv data/limpieza/limpio.csv
``` 

#### Obtención de códigos de empresa
```code
python 02_codificador.py data/limpieza/limpio.csv data/codigos
``` 

#### Obtención de la información usando los códigos obtenidos
```code
python 03_info.py data/codigos data/info
``` 

#### Consolidación de información con archivo limpio
```code
python 04_limpiador_info.py data/info data/info_final
``` 
