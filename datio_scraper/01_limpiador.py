# oga_bg
# Limpieza de datos de entrada
# Se observa que el nombre de la empresa tiene basura
# se debe separar el tipo de sociedad para que la búsqueda se más precisa
import pandas as pd
import sys
import os
import re
import time

if __name__ == "__main__":
    # Parámetros de entrada archivos de entrada y salida
    args = len(sys.argv) - 1
    if(args < 2):
        print("Uso: 01_limpiador.py [archivo de entrada] [archivo de salida]")
        exit()

    inicio = time.time()

    archivo_entrada = sys.argv[1]
    archivo_salida = sys.argv[2]

    # Leer archivo
    datos = pd.read_csv(archivo_entrada)

    # Rellena los Nulos con ''
    rellenar = [
        'Nombre1',
        'Nombre2',
        'Nombre3',
        'Direccion1',
        'Direccion2',
        'Direccion3'
    ]

    for r in rellenar:
        datos[r] = datos[r].fillna('')

    datos['Nombre1'] = datos['Nombre1'].astype(str)
    datos['Nombre2'] = datos['Nombre2'].astype(str)
    datos['Nombre3'] = datos['Nombre3'].astype(str)

    def nombre_completo(row):
        return row['Nombre1'] + row['Nombre2'] + row['Nombre3']

    datos['nombre_completo'] = datos.apply(nombre_completo, axis=1)

    # Limpieza del nombre
    def limpieza(texto):
        patrones = [
            "(SA DE CV)",
            "(SA CV)",
            "(S.A. DE CV)",
            "(S.A DE CV)",
            "(S.A.DE CV)",
            "(SADECV)",
            "(SADE CV)",
            "(SA DECV)",
            "(S DE RL DE CV)",
            "(S DERL DE CV)",
            "(SRL DE CV)",
            "(SDE RL DE CV)",
            "(SC DE RL DE CV)",
            "(S DERL DE CV)",
            "(SPR DE RL DE CV)",
            "(SPR DERL DE CV)"
        ]
        
        n1 = texto.replace("#", "")
        n2 = n1.replace(",", "")
        
        pat_ = "|".join(patrones)
        n3 = re.sub(pat_, "", n2)
        
        return n3

    datos['nombre_completo_limpio'] = datos['nombre_completo'].map(limpieza)

    # Escribir en archivo de salida
    datos.to_csv(archivo_salida, index=False)
    tiempo = time.time() - inicio
    print("Tiempo total: {:.4f} seg.".format(tiempo))