# oga_bg
# Utilizando la API de consulta DENUE del INEGI
# se realiza una búsqueda por nombre del establecimiento
# para obtener códigos que nos servirán para realizar la 
# consulta de información
import pandas as pd
import sys
import os
import time
from time import sleep
import requests

# Función para guardar los datos
def datos_base_con_cp(nombre):
    conteo = 0
    var_ = [
        'Txt_gral',
        'Txt_calle',
        'Txt_colonia',
        'Txt_cp',
        'Txt_vc',
        'Txt_prod',
        'Txt_paisexp',
        'Txt_paisimp',
        'Txt_camara',
        'Txt_mts',
        'Txt_radio',
        'Txt_coord'
    ]

    var_qs = '=&'.join(var_)
    url_sin_cp = "=&Txt_nom=" + nombre
    url_ = 'https://www.inegi.org.mx/app/mapa/componente/Espacial.ashx?metodo=ArmabusquedaEstablecimientos&Ac_eco%5B0%5D%5Bsec%5D=&Ac_eco%5B1%5D%5Bsubsec%5D=&Ac_eco%5B2%5D%5Brama%5D=&Ac_eco%5B3%5D%5Bsubra%5D=&Ac_eco%5B4%5D%5Bclase%5D=&Tama%C3%B1o=&Cve_geo%5B0%5D%5Bent%5D=&Cve_geo%5B1%5D%5Bmun%5D=&Cve_geo%5B2%5D%5Bloc%5D=&'
    url = url_ + var_qs + url_sin_cp + '&idee=&Pagini=1&Pagfin=50'
    respuesta = requests.post(url)
    codigos = []
    if len(respuesta.json()) and 'obj' in respuesta.json():
        conteo = 0
        try:
            conteo = int(respuesta.json()['obj'][-1][0])
        except:
            conteo = 0
        
        if conteo > 0:
            # Elimina el último
            datos = respuesta.json()['obj'][:-1]
            for dato in datos:
                codigo_1 = dato[0]
                codigo_2 = dato[3]
                print(codigo_1, codigo_2)
                codigos.append((codigo_1, codigo_2))

        # print(respuesta.json()['obj'])
    return codigos

if __name__ == "__main__":
    # Parámetros de entrada archivos de entrada y salida
    args = len(sys.argv) - 1
    if(args < 2):
        print("Uso: 02_codificador.py [archivo de entrada] [carpeta de salida]")
        exit()

    inicio = time.time()

    archivo_entrada = sys.argv[1]
    carpeta_salida = sys.argv[2]


    data = pd.read_csv(archivo_entrada)
    data_ = data[['Index', 'nombre_completo_limpio']].copy()

    ultimo_error = 0
    con_errores = []
    max_intentos = 3
    conteo_intentos = 0

    while True:
        try:
            data__ = data_[ultimo_error:]
            archivo = f"{carpeta_salida}/codigos_{ultimo_error}.csv"
            print(f"Archivo: {archivo}")
            with open(archivo, 'w') as f:
                f.write("Index,codigo_1,codigo_2\n")
                for idx, row in data__.iterrows():
                    ultimo_error = idx
                    print(f"Procesando idx: {row['Index']}")
                    codigos = datos_base_con_cp(row['nombre_completo_limpio'])
                    if len(codigos):
                        for codigo_1, codigo_2 in codigos:
                            f.write(f"{row['Index']},{str(codigo_1)},{str(codigo_2)}\n")
            break
        except:
            print(f"Pausando Ejecución - Último Índice: {ultimo_error}")
            conteo_intentos += 1
            if conteo_intentos >= max_intentos:
                conteo_intentos = 0
                con_errores.append(ultimo_error)
                ultimo_error += 1
            sleep(5)    

    # Almacena los idx que tuvieron error en un archivo
    if len(con_errores):
        err = pd.DataFrame(
            con_errores
        )
        err.columns = 'idx'
        err.to_csv('codigos_errores.csv', index=False)

    tiempo = time.time() - inicio
    print("Tiempo total: {:.4f} seg.".format(tiempo))        

