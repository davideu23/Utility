# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 20:43:32 2021

@author: david
"""
#### IMPORTS ####

import re
from time import sleep
from typing import Union, Tuple, List, Optional

#### CLASS ####


class ExitLoopError(BaseException): pass
class TownNotFounded(BaseException): pass

#### FUNCTIONS ####

## PROMT FUNCTIONS ##


def exit_fun():
    raise ExitLoopError()


def null_value():
    print("\nNon hai selezionato un input valido. Prego Riprovare")
    sleep(0.5)


def exec_program(string: str, switcher: dict):
    while True:
        print(string)
        try:
            switcher.get(input("Selezione: "), null_value)()
        except ExitLoopError:
            break

## MAIN FUNCTIONS ##

# Genera una lista o lista di liste da un file di testo (non codificato)
def genfromtxt(nfile: str, sep: str = ';', header: int = 0, deletechars: str = '\n', cols:Optional[Union[None, Tuple[int, int, int], List[int]]] = None) -> List[List[str]]:
    with open(nfile) as f:
        if cols == None:
            return [record.replace(deletechars, '').split(sep) for record in f.readlines()[header:]]
        return [record.replace(deletechars, '').split(sep)[cols[0]:cols[1]:cols[2]] for record in f.readlines()[header:]]


# Rende una lista di liste una unica lista
def flat(lista: List[List]) -> List:
    return [j for i in lista for j in i]


# Crea il codice sia per il nome che per il cognome
def get_name(stringa: str, nome: bool = True) -> str:
    vocali, consonanti = 'AEIOU', 'BCDFGHJKLMNPQRSTVWXYZ'  # assegnazione doppia
    # consonanti e vocali nel nome
    cons, voc = ''.join(c for c in stringa.upper() if c in consonanti), ''.join(
        c for c in stringa.upper() if c in vocali)
    # se le consonanti sono maggiori di 3 ed è per il nome allora restituisce la consonante con indice 0,2 e l'ultima
    if len(cons) > 3 and nome:
        return cons[0:3:2] + cons[3]
    # restituisci i primi 3 caratteri di consonanti + vocali + caratteri speciali
    return (cons + voc + 'XX')[:3]


# Crea il codice per la data di nascita
def get_data(year: str, month: str, day: str, gender: str) -> str:
    months = 'ABCDEHLMPRST'  # codice assegnato ad ogni mese
    # se è maschio aggiunge lo 0 prima della cifra se serve, se è femmina al giorno si aggiunge 40
    day = f'{int(day):02d}' if gender.upper() == "M" else f"{int(day) + 40}"
    # restituisce una stringa contenente le ultime 2 cifre dell'anno, la lettera del mese e il giorno
    return f"{year[2:]}{months[int(month) - 1]}{day}"


# Crea il codice per il comune
def get_town(comune: str) -> str:
    com = flat(genfromtxt(r'Data\CFCodiceComuniItalia.csv', sep=';', header=2,
                          deletechars='\n', cols=(0, 3, 2)))  # genera dal file una lista
    # creo il dizionario chiave:valore dove chiave è il comune e il valore è il codice assegnato ad esso
    dict_com = {k: v for k, v in zip(com[1::2], com[::2])}
    if comune.upper() not in dict_com.keys():
        raise TownNotFounded(f'Town {comune} not found')  # controllo se il comune esiste
    # restituisci il codice assegnato al comune
    return dict_com[comune.upper()]


# Definisce il carattere di controllo, ultimo carattere del codice fiscale
def get_remainder(cod: str) -> str:
    # controllo se il codice passato contiene 15 caratteri, generalmente è sempre così
    if len(cod) != 15:
        raise Exception('ERRORE NELLA FORMULAZIONE DEL CODICE')
    # Genera 3 liste da 3 file diversi
    pari, disp, resto = flat(genfromtxt(r'Data\CFTabellaPari.tsv', '\t', 1, '\n')), flat(genfromtxt(
        r'Data\CFTabellaDispari.tsv', '\t', 1, '\n')), flat(genfromtxt(r'Data\CFTabellaResto.tsv', '\t', 0, '\n'))
    # Crea 3 dizionari chiave:valore assegnando ad ogni carattere un valore numerico
    dic_pari, dic_disp, dict_resto = {k: v for k, v in zip(pari[::2], pari[1::2])}, {
        k: v for k, v in zip(disp[::2], disp[1::2])}, {k: v for k, v in zip(resto[::2], resto[1::2])}
    # lista contenente il valore numerico assegnato per ogni carattere
    chrs = [int(dic_pari[key]) for key in cod[1::2] if key in dic_pari.keys(
    )] + [int(dic_disp[key]) for key in cod[::2] if key in dic_disp.keys()]
    # restituisco il carattere di controllo
    return dict_resto[str(sum(chrs) % 26)]


# Genera e salva codice fiscale
def get_fiscal_code(surname:str, name:str, year:str, month:str, day:str, gender:str, town:str, save:Optional[bool]=False) -> str:
    # genera il codice fiscale senza l'ultimo carattere
    codice_fiscale = f"{get_name(surname, False)}{get_name(name)}{get_data(year, month, day, gender)}"
    try: codice_fiscale += f"{get_town(town)}"
    except TownNotFounded: codice_fiscale += f"{get_town(town)}"
    # restituisce il codice fiscale + carattere di controllo
    codice_fiscale = codice_fiscale + get_remainder(codice_fiscale)
    if save:
        write_codice_fiscale(
            [surname, name, year, month, day, gender, town, codice_fiscale])
    return codice_fiscale


# Salva su file una lista
def write_codice_fiscale(lista:List[str]) -> None:
    writable = ';'.join(i.upper() for i in lista)
    _header = "Surname;Name;Year;Month;Day;Gender;Town;Fiscal_code"

    # if os.path.isfile(output_files["fiscal_code"]):
    with open(output_files["fiscal_code"], "a+") as f:
        f.seek(0) #posiziona puntatore a riga 0, colonna 0
        if len(f.readlines()) == 0: # Leggendo il file il puntatore si posiziona alla fine del file
            f.write(_header)
        f.write(f"\n{writable}")
    # else: pass


def get_input(question: str, validation: str):
    """
    ask a question and validate the input

    Parameters
    ----------
    question : str
        question string to display.
    validation : str
        regex string used to validate input.

    Returns
    -------
    str
        the value inserted.

    """
    value = input(f"Insert {question}: ")
    if control_regex(value, validation):
        return value
    print("Input non valid. Retry...")
    return get_input(question, validation)


def control_regex(value: str, re_str: str) --> bool:
    """
    Controls if the string match the regex passed

    Parameters
    ----------
    value : str
        string to analyze.
    re_str : str
        regex string to test.

    Returns
    -------
    bool
        True or False.
    """
    if re.compile(re_str).match(value):
        return True
    return False


def get_params(save=False):
    _par = [{"question": "Surname", "validation": regex_control["str"]},
            {"question": "Name", "validation": regex_control["str"]},
            {"question": "Birth year(number)",
             "validation": regex_control["year"]},
            {"question": "Birth month(number)",
             "validation": regex_control["month"]},
            {"question": "Birth day(number)",
             "validation": regex_control["day"]},
            {"question": "Gender [M/F]",
                "validation": regex_control["gender"]},
            {"question": "Town of birth", "validation": regex_control["str"]}]
    return [get_input(**par) for par in _par] + [save]


def read_file_promt():
    welcome_string = '\n1.Search by exact term\n2.Back'
    switcher_case = {"1": search_by_term,
                     "2": exit_fun}
    exec_program(welcome_string, switcher_case)
    
def search_by_term():
    file = genfromtxt(output_files["fiscal_code"])
    columns = file[0]
    dict_choice = {f"{k+1}":v for k,v in enumerate(columns)}
    print('\r\n'.join(f"{k}. {v}" for k, v in dict_choice.items()))
    selection = input("Selezione: ")
    if selection not in dict_choice.keys(): return search_by_term()
    term = input(f'Valore da cercare per {dict_choice[selection]}: ').upper()
    query = list(filter(lambda x: x[int(selection) - 1] == term, file[1:]))
    if len(query) > 0: 
        ##print('0. ' + '\t'.join(columns)) #TODO -- IMPAGINAZIONE
        print('\r\n'.join(map(lambda x: f"{x[0] + 1}. " + "\t".join(x[1]), enumerate(query))))
    else: print(f'No result found for query: {dict_choice[selection]} = {term}.')
    
def compute_fiscal_code():
    print(f"Fiscal code generated: {get_fiscal_code(*get_params())}")


def compute_and_save_fiscal_code():
    print(f"Fiscal code generated: {get_fiscal_code(*get_params(True))}")


### MAIN ###
if __name__ == "__main__":
    welcome_string = '\n1.Compute fiscal code\n2.Compute and save fiscal code\n3.Read File\n4.Exit'
    switcher_case = {"1": compute_fiscal_code,
                     "2": compute_and_save_fiscal_code, "3": read_file_promt ,"4": exit_fun}
    regex_control = {"str": "^[A-z\s']+$",
                     "year": "^((?:1(?:8|9)\d{2})|(?:20(?:(?:(?:0|1)\d)|(?:2[0-2]))))$",
                     "month": "^((?:0?[1-9])|(?:1(?:[0-2])))$",
                     "day": "^((?:0?[1-9])|(?:1\d)|(?:2\d)|(?:3[0-1]))$",
                     "gender": "^(M|F|m|f)$"}

    output_files = {"fiscal_code": "fiscal_code.csv"}
    print("This script aim to compute the Italian fiscal code. See https://en.wikipedia.org/wiki/Italian_fiscal_code for further info.")
    print("DISCLAIMER: In some cases, when there are 2 or more people born in the same town on the " +
          "same day with same name and surname, this script can fail beacause fiscal code is a primary key " +
          "and duplicated values are no permitted, so, the municipality that have access to fiscal code DB " +
          "changes it to makes it unique and I have no such informations.")
    exec_program(welcome_string, switcher_case)
