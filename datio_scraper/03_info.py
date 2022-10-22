# oga_bg
# Utilizando la API de consulta DENUE del INEGI
# se realiza una extraccion de información
import pandas as pd
import sys
import os
import time
from time import sleep
import requests

def obtener_informacion(codigo_1, codigo_2):
    url = 'https://www.inegi.org.mx/app/mapa/componente/APIGeografico/infowindowDenue/' + \
    str(codigo_1) + \
    '/DENUE/'+ \
    str(codigo_2)
    respuesta = requests.get(url)
    info = {}
    r_ = respuesta.json()[0]
    if respuesta is not None and "d1" in r_:
        info = {
            'id_establecimiento': r_['d1'],
            'cve_ent': r_['d2'],
            'entidad': r_['d3'],
            'cve_mun': r_['d4'],
            'municipio': r_['d5'],
            'cve_loc': r_['d6'],
            'localidad': r_['d7'],
            'nom_estab': r_['d8'],
            'raz_social': r_['d9'],
            'codigo_act': r_['d11'],
            'nombre_act': r_['d12'],
            'per_ocu': r_['d14'],
            'tipo_vial': r_['d15'],
            'nom_vial': r_['d16'],
            'numero_ext': r_['d17'],
            'edificio': r_['d18'],
            'numero_int': r_['d19'],
            'tipo_asent': r_['d20'],
            'nomb_asent': r_['d21'],
            'nom_CenCom': r_['d22'],
            'num_local': r_['d23'],
            'cod_postal': r_['d24'],
            'ageb': r_['d25'],
            'manzana': r_['d26'],
            'telefono': r_['d27'],
            'correoelec': r_['d28'],
            'www': r_['d30'],
            'fecha_alta': r_['d33'],
            'latitud': r_['d34'],
            'longitud': r_['d35'],
            'tipo_asent': r_['d57'],
            'tipoCenCom': r_['d58'],
            'tipo_v_e_1': r_['d61'],
            'nom_v_e_1': r_['d62'],
            'tipo_v_e_2': r_['d63'],
            'nom_v_e_2': r_['d64'],
            'tipo_v_e_3': r_['d65'],
            'nom_v_e_3': r_['d66'],
            'clee': r_['d68']
        }
    # print(info)
    return info

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
        d0 = pd.read_csv(arch)
        data.append(d0)

    if len(data):
        codigos = pd.concat(data, ignore_index=True, sort=False)
        codigos['idx_codigo'] = codigos.index
        # Guarda archivo temporal
        codigos.to_csv('codigos_tmp.csv', index=False)

        # Realiza el proceso de extracción de información
        data_ = codigos.copy()
        ultimo_error = 0
        max_intentos = 5
        conteo_intentos = 0
        con_errores = []

        while True:
            try:
                data__ = data_[ultimo_error:]
                archivo = f"{carpeta_salida}/info_{ultimo_error}.csv"
                print(f"Archivo: {archivo}")
                i = 0
                with open(archivo, 'w') as f:
                    for idx, row in data__.iterrows():
                        # print(row)
                        ultimo_error = idx
                        print(f"Procesando idx_codigo: {row['idx_codigo']}")
                        info = obtener_informacion(int(row['codigo_1']), int(row['codigo_2']))
                        if info:
                            if idx == 0:
                                k_ = list(info.keys())
                                st_ = "Index|codigo_1|codigo_2|idx_codigo|" + "|".join(k_)
                                f.write(f"{st_}\n")
                            inf_ = info.values()
                            st_ = f"{row['Index']}|{row['codigo_1']}|{row['codigo_2']}|{row['idx_codigo']}|" + "|".join(inf_)
                            f.write(f"{st_}\n")
                break
            except Exception as e:
                print(repr(e))
                
                print(f"Pausando Ejecución - Último Índice: {ultimo_error}")
                conteo_intentos += 1
                if conteo_intentos >= max_intentos:
                    conteo_intentos = 0
                    con_errores.append(ultimo_error)
                    ultimo_error += 1
                sleep(10)
        
    tiempo = time.time() - inicio
    print("Tiempo total: {:.4f} seg.".format(tiempo))        

