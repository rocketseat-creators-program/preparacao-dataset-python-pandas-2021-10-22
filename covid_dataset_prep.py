# -*- coding: utf-8 -*-
import os
import pandas as pd
from opencage.geocoder import OpenCageGeocode
# O arquivo key.secret é um arquivo texto com a chave para acesso
# à API do site opencage
opencagekey = open('key.secret', 'r')
key = opencagekey.read().replace('\n', '')
opencagekey.close()
#######################################################################
# Carga e tratamento do dataset
def prepareData(fileName):
    # Carregar o arquivo csv como dataframe pandas, somente colunas que interessam
    dataset = pd.read_csv(fileName, encoding = 'utf-8', usecols=[\
        'city',
        'city_ibge_code',
        'date',
        'epidemiological_week',
        'estimated_population',
        'last_available_confirmed',
        'last_available_confirmed_per_100k_inhabitants',
        'last_available_deaths',
        'place_type',
        'state',
        'new_confirmed',
        'new_deaths'])
    # Renomear as colunas do dataframe                        
    dataset = dataset.rename(columns = {\
        'city':                     'muni',
        'city_ibge_code':           'codIbge',
        'date':                     'data',
        'epidemiological_week':     'semEpid',
        'estimated_population':     'popEstim',
        'last_available_confirmed': 'confAcc',
        'last_available_confirmed_per_100k_inhabitants': 'confAcc100k',
        'last_available_deaths':    'obitoAcc',
        'place_type':               'tipoLocal',
        'state':                    'uf',
        'new_confirmed':            'confDia',
        'new_deaths':               'obitoDia'})
    # Remover linhas em que o nome do município está como NA
    dataset.dropna(subset = ['muni', 'codIbge'], inplace = True)
    # Define o tipo de agregação de cada coluna
    aggregation = {
        'muni':        'first', # Primeiro valor encontrado
        'codIbge':     'first', # Primeiro valor encontrado
        'data':        'last',  # Ultimo valor encontrado
        'semEpid':     'first', # Primeiro valor encontrado
        'popEstim':    'max',   # Maximo valor do conjunto
        'confAcc':     'max',   # Maximo valor do conjunto
        'confAcc100k': 'max',   # Maximo valor do conjunto
        'obitoAcc':    'max',   # Maximo valor do conjunto
        'tipoLocal':   'first', # Primeiro valor encontrado
        'uf':          'first', # Primeiro valor encontrado
        'confDia':     'sum',   # Soma dos valores do conjunto
        'obitoDia':    'sum'}   # Soma dos valores do conjunto
    # Agrega resultados da semana epidemiológica por município
    dataset = dataset.groupby(['semEpid', 'codIbge']).aggregate(aggregation)\
                     .reindex(columns = dataset.columns)
    dataset.reset_index(drop = True, inplace = True)
    # # retorna o dataset tratado
    return dataset
#######################################################################
# Cria set de municipios para coleta de geolocalização
def getCitySet(dataset):
    if not os.path.exists(dataset):
        colNames = ['muni', 'codIbge', 'uf', 'lat', 'lon']
        geoloc = pd.DataFrame(columns = colNames)
        geoloc.to_csv(dataset, encoding = 'utf-8')
    # Carregar o arquivo csv com a lista parcial de municípios com geolocalização
    # como dataframe pandas, somente colunas que interessam
    geoloc = pd.read_csv(dataset, encoding = 'utf-8', usecols=['muni', 'codIbge', 'uf', 'lat', 'lon'])
    # # retorna o dataset tratado
    return geoloc
#######################################################################
def getGeoLocation(cidade, estado, pais = 'brasil'):
    query = (str(cidade.lower()) + ', ' + str(estado.lower()) + ', ' + str(pais.lower()))
    geocoder = OpenCageGeocode(key)
    coord = geocoder.geocode(query)
    resposta = {
        'lat': coord[0]['geometry']['lat'],
        'lon': coord[0]['geometry']['lng']}
    return resposta
#######################################################################
ds1 = prepareData('caso_full.csv')
print(ds1.shape[0])
ds2 = getCitySet('muniset.csv')
joinDataset = pd.merge(ds1, ds2, on = ['muni', 'codIbge', 'uf'], how = 'left', indicator = True)
joinDataset.reset_index(drop = True, inplace = True)
getgeo = []
for index, rows in joinDataset.iterrows():
    if (rows._merge == 'left_only') and ([rows.muni, rows.codIbge, rows.uf] not in getgeo):
        getgeo.append([rows.muni, rows.codIbge, rows.uf])
        resposta = getGeoLocation(rows.muni, rows.uf)
        ds2 = ds2.append(pd.Series([
            rows.muni,
            rows.codIbge,
            rows.uf,
            resposta['lat'],
            resposta['lon']], index = ds2.columns), ignore_index = True)
        print('Municipio adicionado: ', rows.muni, rows.codIbge, rows.uf, resposta['lat'], resposta['lon'])
        joinDataset['lat'] = resposta['lat']
        joinDataset['lon'] = resposta['lon']
if len(getgeo) > 0:
    ds2.to_csv('muniset.csv', encoding = 'utf-8')
joinDataset.to_csv('covid_dataset.csv', encoding = 'utf-8')
print('fim')
