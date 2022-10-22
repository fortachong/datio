# oga_bg
# Se toman ciertas columnas de información, no todas
import pandas as pd
import numpy as np
import sys
import os
import time
from time import sleep
import re

def min_max_ocupacion(st):
    minimo = np.NINF
    maximo = np.PINF
    m = re.findall("[0-9]+", st)
    if len(m) == 1: 
        minimo = m[0]
    elif len(m) == 2: 
        minimo = m[0]
        maximo = m[1]
    return minimo, maximo

if __name__ == "__main__":
    # Parámetros de entrada archivos de entrada y salida
    args = len(sys.argv) - 1
    if(args < 2):
        print("Uso: 03_info.py [carpeta de entrada] [carpeta de salida]")
        exit()

    inicio = time.time()

    carpeta_entrada = sys.argv[1]
    carpeta_salida = sys.argv[2] 

    data = []
    archivos_entrada = os.listdir(carpeta_entrada)
    for archivo in archivos_entrada:
        arch = os.path.join(carpeta_entrada, archivo)
        # Leer en pandas y consolidarlos
        d0 = pd.read_csv(arch, sep='|')
        data.append(d0)

    if len(data):
        data_ = pd.concat(data, ignore_index=True, sort=False)

        cols = [
            'Index',
            'codigo_1',
            'codigo_2',
            'idx_codigo',
            'id_establecimiento', 
            'clee', 
            'nom_estab', 
            'raz_social', 
            'entidad', 
            'municipio', 
            'cod_postal', 
            'codigo_act', 
            'nombre_act', 
            'fecha_alta', 
            'www', 
            'latitud', 
            'longitud', 
            'per_ocu'
        ]
        
        d0 = data_[cols].copy()
        d0['entidad'] = d0['entidad'].str.strip()
        d0['min_ocupacion'] = d0['per_ocu'].map(lambda x: min_max_ocupacion(x)[0])
        d0['max_ocupacion'] = d0['per_ocu'].map(lambda x: min_max_ocupacion(x)[1])

        d0.to_csv(f"{carpeta_salida}/info_final.csv", sep="|", index=False)

    tiempo = time.time() - inicio
    print("Tiempo total: {:.4f} seg.".format(tiempo))        

