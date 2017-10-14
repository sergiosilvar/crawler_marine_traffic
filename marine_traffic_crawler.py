
# coding: utf-8

# In[257]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from pathlib import Path
import time


logger = logging.getLogger(__name__)
URL_BASE = 'http://www.marinetraffic.com'


data_coleta = time.strftime("%x")

def obtem_pagina(url):
    user_agent = {'User-agent': 'Mozilla/5.0'}
    return requests.get(url, headers = user_agent,)

def cria_pasta(caminho_arquivo):
    pasta = caminho_arquivo.parent
    if not pasta.exists():
        pasta.mkdir(parents=True)

# # Navios de interesse

# In[80]:
def crawl_navios_interesse(arquivo_csv='./output/navios_interesse.csv'):

    urls = ['https://www.marinetraffic.com/en/ais/details/ships/shipid:211947/mmsi:240069000/vessel:ELKA%20ARISTOTLE',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:375133/mmsi:311585000/vessel:NORDIC%20RIO',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:374016/mmsi:311067700/vessel:CARTOLA',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:374017/mmsi:311067800/vessel:ATAULFO%20ALVES',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:775266/imo:9453822/mmsi:710016250/vessel:DRAGAO%20DO%20MAR',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:774708/mmsi:710020930/vessel:HENRIQUE%20DIAS',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:3733074/mmsi:710025780/vessel:JOSE%20DO%20PATROCINIO',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:4237514/imo:9453872/mmsi:710028630/vessel:MACHADO%20DE%20ASSIS',
    'https://www.marinetraffic.com/pt/ais/details/ships/9453315/vessel:JOAO_CANDIDO',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:3563187/imo:9453858/mmsi:710024830/vessel:MARCILIO%20DIAS',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:4796933/imo:9453884/mmsi:710030790/vessel:MILTON%20SANTOS',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:368678/mmsi:308293000/imo:9308077/vessel:NAVION_GOTHENBURG',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:375128/imo:9248435/mmsi:311582000/vessel:NAVION%20STAVANGER',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:375130/mmsi:311584000/vessel:NORDIC%20BRASILIA',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:374858/mmsi:311435000/imo:9208045/vessel:NORDIC_SPIRIT',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:374925/mmsi:311471000/vessel:STENA%20SPIRIT',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:373974/mmsi:311066100/imo:9308065/vessel:STORVIKEN',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:775508/mmsi:710239000/vessel:ZUMBI%20DOS%20PALMARES',
    'https://www.marinetraffic.com/pt/ais/details/ships/shipid:2982008/mmsi:710023040/vessel:ANDRE%20REBOUCAS',
    'https://www.marinetraffic.com/en/ais/details/ships/shipid:279579/mmsi:247241200/vessel:EXCALIBUR',
    'https://www.marinetraffic.com/en/ais/details/ships/shipid:2060845/mmsi:311000285/vessel:ESSHU%20MARU',
    'https://www.marinetraffic.com/en/ais/details/ships/shipid:114502/mmsi:205423000/vessel:EXCELSIOR',
    'https://www.marinetraffic.com/en/ais/details/ships/shipid:3058/mmsi:229673000/vessel:COOL%20RUNNER',
    'https://www.marinetraffic.com/en/ais/details/ships/shipid:713685/mmsi:538004501/vessel:EXPERIENCE',
    'https://www.marinetraffic.com/en/ais/details/ships/shipid:29755/mmsi:7100079/vessel:PLATAFORMA%20MERLUZA']


    #urls = [i for i in urls if i.find('MILTON')>0]
    navios = []
    navios_erro = []
    for url in urls:
        try:
            r = obtem_pagina(url)
            soup = BeautifulSoup(r.text, 'lxml')
            detalhes = []

            # Nome do navio
            nome = soup.find('h1', class_='font-200 no-margin').text
            detalhes.append(nome)

            div = soup.find('div', class_='row equal-height')
            div_infos = div.find_all('div', class_='col-xs-6')
            for div in div_infos:
                detalhes.extend([i.text for i in div.find_all('b')])
            detalhes.append(data_coleta)
            navios.append(detalhes)
        except Exception as e:
            navios_erro.append([str(e),url])




    # In[85]:


    logger.info('Total de navios sem erro / com erros: {} / {}'.format(len(navios),len(navios_erro)))

    df = pd.DataFrame(navios, columns= ['Nome', 'IMO', 'MMSI', 'Indicativo', 'Bandeira',
            'Tipo', 'Tonelagem', 'Porte', 'Comp_Larg', 'Ano', 'Estado', 'Data'])

    # Salva arquivo no diretório indicado.
    caminho_arquivo = Path(arquivo_csv)
    cria_pasta(caminho_arquivo)
    df.to_csv(caminho_arquivo.as_posix(), sep=';', index=False)

    logger.info('Arquivo {} criado.'.format(caminho_arquivo.as_posix()))

# # Portos brasileiros

# In[236]:

def crawl_portos_brasil(arquivo_csv='./output/portos.csv'):
    url = 'https://www.marinetraffic.com/en/ais/index/ports/all/flag:BR/port_type:p'

    tabela_portos = []

    while True:
        logger.info('Capturar portos em: {}'.format(url))
        html_portos = obtem_pagina(url).text
        soup = BeautifulSoup(html_portos, 'lxml')

        # Tag <table> dos portos.
        table_portos = soup.find('table', class_='table table-hover text-left')

        # Percorrer todas as linhas da tabela.
        # A primeira linha é o cabeçalho, então iremos pulá-la.
        for linha in table_portos.find_all('tr')[1:]:

            # Cada linha contém uma lista de células com os valores de interesse.
            celulas = linha.find_all('td')

            # Coluna da bandeira do país.
            col = celulas[0]
            pais = col.img.attrs['title']
            link_bandeira_pais = col.img['src']

            # Coluna de link para o porto.
            col = celulas[1]
            link_porto = col.a['href']
            nome_porto = col.text.strip()

            # Coluna Codigo.
            col = celulas[2]
            codigo = col.text.strip()

            # Coluna Foto.
            col = celulas[3]
            link_fotos = col.a['href']

            # Coluna Tipo
            col = celulas[4]
            tipo = col.text.strip()

            # Coluna link para mapa do porto.
            col = celulas[5]
            link_mapa_porto = col.a['href']

            # Coluna Navios no porto.
            col = celulas[6]
            link_navios_porto = col.a['href']

            # Coluna link partidas.
            col = celulas[7]
            link_partidas = col.a['href']

            # Coluna link chegadas.
            col = celulas[8]
            link_chegadas = col.a['href']

            # Coluna link chegadas esperadas.
            col = celulas[9]
            link_chegadas_esperadas = col.a['href']

            # Coluna status da cobertura AIS.
            col = celulas[10]
            cobertura_ais = col.div['title']

            # Armazena os dados de cada porto na tabela de portos.
            dados = [pais, nome_porto,codigo, tipo, cobertura_ais, link_bandeira_pais, link_navios_porto,
                     link_chegadas_esperadas, link_chegadas, link_porto, link_fotos,
                     link_mapa_porto,]
            tabela_portos.append(dados)

        # Não há próxima página?
        next_disabled = soup.find('span', class_='next disabled')
        if next_disabled:
            logger.info('Fim da captura de portos.')

            break
        else:
            next_page = soup.find('span', class_='next')
            url = URL_BASE + next_page.a['href']

    cabecalho = ['Pais','Nome','Codigo','Tipo','CoberturaAIS','LinkBandeira','LinkNaviosPorto',
                 'LinkChegadasEsperadas','LinkChegadas','LinkPorto','LinkFotos',
                'LinkMapaPorto']
    df = pd.DataFrame(tabela_portos, columns=cabecalho)
    caminho_arquivo = Path(arquivo_csv)
    cria_pasta(caminho_arquivo)
    df.to_csv(caminho_arquivo.as_posix(), sep=';', index=False)

if __name__ =='__main__':
    #logging.basicConfig(filename='marine_traffic.log', filemode='w')
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    crawl_portos_brasil()
