"""Taller presencial"""



# pylint: disable=broad-exception-raised 

import fileinput
import glob
import os
import os.path	
import string
import time
from collections import defaultdict
from multiprocessing import Pool

from toolz.itertoolz import concat # type: ignore



#
# Copia de archivos
#
def copy_raw_files_to_input_folder(n):
    """Copia los archivos de la carpeta raw a la carpeta input"""
    create_directory("files/input")
   
    for file in glob.glob("files/raw/*"):
        for i in range(1, n+1): 
            with open(file, "r", encoding= "utf-8") as f:
                with open(
                    f"files/input/{os.path.basename(file).split('.')[0]}_{i}.txt", 
                    "w", 
                    encoding= "utf-8"
                    ) as f2:
                    f2.write(f.read())


#
# Lectura de archivos
#
def load_input(input_directory):
    """Carga los archivos de la carpeta input y devuelve una lista de cadenas"""

    def make_iterator_from_single_file(input_directory):
        """Crea un iterador a partir de un solo archivo"""
        with open(input_directory, "r", encoding= "utf-8") as file:
            yield from file

    def make_iterator_from_multiple_files(input_directory):
        """Crea un iterador a partir de varios archivos"""
        input_directory = os.path.join(input_directory, "*")
        files = glob.glob(input_directory)
        with fileinput.input(files = files) as file:
            yield from file

    if os.path.isfile(input_directory):
        return make_iterator_from_single_file(input_directory)
    return make_iterator_from_multiple_files(input_directory)


#
# Preprocesamiento
#   
def preprocessing(x):
    """Preprocesa la linea x"""
    x = x.lower()
    x = x.translate(str.maketrans("", "", string.punctuation))
    x = x.replace("\n", "")
    return x

def line_preprocessing(sequence):
    """Preprocesa la linea y devuelve una lista de palabras"""
    with Pool() as pool:
        return pool.map(preprocessing, sequence)
    

#
# Mapper
#
def map_line(x):
    """Aplica el mapeo a la linea x"""
    return[(w,1) for w in x.split()] # devuelve una lista de tuplas (palabra, 1)

def mapper(sequence):
    """Aplica el mapeo a la secuencia de palabras"""
    with Pool() as pool:
        sequence = pool.map(map_line, sequence)
        sequence = concat(sequence) # aplana la lista de listas
    return sequence


#
# Shuffle and Sort
#   
def shuffle_and_sort(sequence):
    """Agrupa las palabras por clave y las ordena"""
    return sorted(sequence, key = lambda x: x[0]) 

#
# Reducer
#
def sum_by_key(chunk):
    """Suma los valores por clave"""
    result = defaultdict(int)
    for key, value in chunk:
        result[key] += value
    return list(result.items())

def reducer(sequence):
    """Aplica la reducción a la secuencia de palabras"""

    def chunkify(sequence, num_chunks):
        """Divide la secuencia en num_chunks partes"""
        return [sequence[i::num_chunks] for i in range(num_chunks)]
    
    def merge_results(chunks):
        """Fusiona los resultados de los chunks"""
        final_result = defaultdict(int)
        for chunk in chunks:
            for key, value in chunk:
                final_result[key] += value
        return list(final_result.items())
    
    num_chunks = os.cpu_count() # numero de nucleos
    chunks = chunkify(sequence, num_chunks) # divide la secuencia en partes

    with Pool(num_chunks) as pool:
        chunk_results = pool.map(sum_by_key, chunks) # aplica la reduccion a cada parte

    return merge_results(chunk_results) # fusiona los resultados de los chunks


#
# Crea un directorio
#
def create_directory(directory):
    """Crea el directorio de salida si no existe"""
    if os.path.exists(directory):
        for file in glob.glob(f"{directory}/*"):
            os.remove(file)
        os.rmdir(directory)
    os.makedirs(directory)


#
# Guarda el resultado en un archivo
#
def save_output(output_directory, sequence):   
    """Guarda el resultado en un archivo"""
    with open(f"{output_directory}/part-00000", "w", encoding= "utf-8") as file:
        for key, value in sequence:
            file.write(f"{key}\t{value}\n")


#
# La siguiente función crea un archivo llamado _SUCCESS en el directorio de salida
#
def create_marker(output_directory):
    """Crea un archivo _SUCCESS en el directorio de salida"""
    with open(f"{output_directory}/_SUCCESS", "w", encoding= "utf-8") as file:
        file.write("")


#
# Job que orquesta el flujo de trabajo
#
def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory) # Carga los archivos de la carpeta input
    sequence = line_preprocessing(sequence) # Preprocesa la secuencia de palabras
    sequence = mapper(sequence) # Aplica el mapeo a la secuencia de palabras
    sequence = shuffle_and_sort(sequence) # Agrupa las palabras por clave y las ordena
    sequence = reducer(sequence) # Aplica la reduccion a la secuencia de palabras

    create_directory(output_directory) # Crea el directorio de salida
    save_output(output_directory, sequence) # Guarda el resultado en un archivo
    create_marker(output_directory) # Crea un archivo _SUCCESS en el directorio de salida


if __name__ == "__main__":
    # Copia los archivos de la carpeta raw a la carpeta input
    copy_raw_files_to_input_folder(1000)

    # Ejecuta el job
    start_time = time.time()

    run_job(
        "files/input", 
        "files/output")
    
    end_time = time.time()
    print(f"Tiempo de ejecucion: {end_time - start_time} segundos")