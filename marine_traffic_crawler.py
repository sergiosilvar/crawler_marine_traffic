# coding: utf-8

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from pathlib import Path
import time
from datetime import datetime



logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)

URL_BASE = 'http://www.marinetraffic.com'




def obtem_pagina(url, proxy = None):
    user_agent = {'User-agent': 'Mozilla/5.0'}
    return requests.get(url, headers = user_agent, proxies = proxy)

def cria_pasta(caminho_arquivo):
    pasta = caminho_arquivo.parent
    if not pasta.exists():
        pasta.mkdir(parents=True)

def data_coleta():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M')

def converte_data(num):
    return time.strftime('%Y-%m-%d %H:%M', time.gmtime(num))

def salva_dataframe_csv(dataframe, caminho_arquivo):
    caminho_arquivo_acum = caminho_arquivo.replace('.csv', '_acumulado.csv')

    dataframe.to_csv(caminho_arquivo_acum, sep=';', index=False, mode='a', decimal=',')
    logger.info('Arquivo {} criado.'.format(caminho_arquivo_acum))

    dataframe.to_csv(caminho_arquivo, sep=';', index=False, mode='w', decimal=',')
    logger.info('Arquivo {} criado.'.format(caminho_arquivo))


# # Navios de interesse

'''
    Crawl dos navios de interesse.

    arquivo_csv - arquivo de saída.
    proxy - proxy se necessário.
'''
def crawl_navios_interesse(arquivo_csv='./output/navios_interesse.csv', proxy=None):

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
            logger.info('Obtendo dados de navio em {}.'.format(url))
            r = obtem_pagina(url, proxy)
            soup = BeautifulSoup(r.text, 'lxml')
            detalhes = []

            # Nome do navio
            nome = soup.find('h1', class_='font-200 no-margin').text
            detalhes.append(nome)

            div = soup.find('div', class_='row equal-height')
            div_infos = div.find_all('div', class_='col-xs-6')
            for div in div_infos:
                detalhes.extend([i.text for i in div.find_all('b')])
            detalhes.append(data_coleta())
            navios.append(detalhes)
        except Exception as e:
            logger.error('Erro ao obter dados do navio {}.'.format(url))
            navios_erro.append([str(e),url])




    # In[85]:


    logger.info('Total de navios sem erro / com erros: {} / {}'.format(len(navios),len(navios_erro)))

    df = pd.DataFrame(navios, columns= ['Nome', 'IMO', 'MMSI', 'Indicativo', 'Bandeira',
            'Tipo', 'Tonelagem', 'Porte', 'Comp_Larg', 'Ano', 'Estado', 'DataColeta'])

    # Salva arquivo no diretório indicado.
    caminho_arquivo = Path(arquivo_csv)
    cria_pasta(caminho_arquivo)
    salva_dataframe_csv(df,caminho_arquivo.as_posix())



# # Portos brasileiros

# In[236]:

def crawl_portos_brasil(arquivo_csv='./output/portos.csv', proxy=None):
    url = 'https://www.marinetraffic.com/en/ais/index/ports/all/flag:BR/port_type:p'

    tabela_portos = []

    while True:
        logger.info('Capturar portos em: {}'.format(url))
        html_portos = obtem_pagina(url, proxy=proxy).text
        soup = BeautifulSoup(html_portos, 'lxml')

        # Tag <table> dos portos.
        table_portos = soup.find('table', class_='table table-hover text-left')

        # Percorrer todas as linhas da tabela.
        # A primeira linha é o cabeçalho, então iremos pulá-la.
        linhas = table_portos.find_all('tr')
        for linha in linhas[1:]:

            # Cada linha contém uma lista de células com os valores de interesse.
            celulas = linha.find_all('td')

            # Pular propagada que contém apenas uma célula <td>.
            if len(celulas) == 1: continue

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
                     link_mapa_porto, data_coleta()]
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
                'LinkMapaPorto', 'DataColeta']
    df = pd.DataFrame(tabela_portos, columns=cabecalho)

    # Issue #3
    df['Id'] = df.LinkPorto.str.extract(r'ports/(\d+)/Brazil')

    caminho_arquivo = Path(arquivo_csv)
    cria_pasta(caminho_arquivo)
    salva_dataframe_csv(df, caminho_arquivo.as_posix())

def crawl_navios_em_portos(arquivo_csv='./output/navios_em_portos.csv', proxy=None):
    tabela_navios_porto = []


    df_portos_interesse = pd.read_csv('./input/portos_interesse.csv', sep=';', encoding='latin-1')
    df_portos = pd.read_csv('./output/portos.csv', sep=';', encoding='latin-1')
    nome_portos_interesse = df_portos_interesse.Nome.values


    for nome_porto in nome_portos_interesse:
        porto = df_portos[df_portos.Nome==nome_porto]
        url_navios_porto = URL_BASE + porto.LinkNaviosPorto.values[0]

        while True:
            logger.info('Capturar navios no porto {}'.format(url_navios_porto))
            html_navios_porto = obtem_pagina(url_navios_porto, proxy=proxy).text
            soup = BeautifulSoup(html_navios_porto, 'lxml')
            # Tag <table> dos navios.
            table = soup.find('table', class_='table table-hover text-left')

            # Percorrer todas as linhas da tabela.
            # A primeira linha é o cabeçalho, então iremos pulá-la.
            linhas = table.find_all('tr')
            for linha in linhas[1:]:

                # Cada linha contém uma lista de células com os valores de interesse.
                celulas = linha.find_all('td')

                # Pular propagada que contém apenas uma célula <td>.
                if len(celulas) == 1: continue

                # Coluna Tipo.
                col = celulas[4]
                tipo = col.text.strip()

                # Se não for do tipo "tanker", pula para próximo navio.
                if tipo.lower().find('tanker') == -1: continue


                # Coluna da bandeira do país.
                col = celulas[0]
                pais = col.img.attrs['title']
                link_bandeira_pais = col.img['src']


                # Coluna de link para o navio.
                col = celulas[1]
                link_navio = col.a['href']
                nome_navio = col.text.strip()

                # Coluna Foto.
                col = celulas[2]
                link_fotos = col.a['href']


                # Coluna Dimensões.
                col = celulas[5]
                dimensoes = col.text.strip()

                # Coluna Porte.
                col = celulas[6]
                porte = col.text.strip()

                # Coluna Data Ultimo Sinal.
                col = celulas[8]
                data_ultimo_sinal = converte_data(int(col.time.text.strip()))

                # Coluna Data Chegada.
                col = celulas[9]
                data_chegada = None
                if col.time:
                    data_chegada = converte_data(int(col.time.text.strip()))

                # Armazena os dados de cada navio na tabela de navios.
                dados = [nome_porto, nome_navio, tipo, pais, dimensoes, porte, data_ultimo_sinal, data_chegada, link_bandeira_pais, link_fotos,data_coleta()]
                tabela_navios_porto.append(dados)

            # Não há próxima página?
            next_disabled = soup.find('span', class_='next disabled')
            if next_disabled:
                logger.info('Fim da captura de navios em portos para o porto {}.'.format(nome_porto))
                break
            elif soup.find('span', class_='next'):
                next_page = soup.find('span', class_='next')
                url_navios_porto = URL_BASE + next_page.a['href']
            else:
                logger.info('Fim da captura de navios em portos para o porto {}.'.format(nome_porto))
                break


    cabecalho = ['Porto', 'Nome','Tipo','Pais', 'Dimensoes', 'Porte', 'DataUltimoSinal', 'DataChegada', 'LinkBandeira','LinkFotos','DataColeta']
    df = pd.DataFrame(tabela_navios_porto, columns=cabecalho)
    caminho_arquivo = Path(arquivo_csv)
    cria_pasta(caminho_arquivo)
    salva_dataframe_csv(df, caminho_arquivo.as_posix())

def crawl_chegadas_esperadas(arquivo_csv='./output/chegadas_esperadas.csv', proxy=None):
    tabela_chegadas_esperadas = []


    df_portos_interesse = pd.read_csv('./input/portos_interesse.csv', sep=';', encoding='latin-1')
    df_portos = pd.read_csv('./output/portos.csv', sep=';', encoding='latin-1')
    nome_portos_interesse = df_portos_interesse.Nome.values


    for nome_porto in nome_portos_interesse:
        porto = df_portos[df_portos.Nome==nome_porto]
        url_chegadas_esperadas = URL_BASE + porto.LinkChegadasEsperadas.values[0]

        while True:
            logger.info('Capturar chegadas esperadas no porto {}'.format(url_chegadas_esperadas))
            html_navios_porto = obtem_pagina(url_chegadas_esperadas,proxy=proxy).text
            soup = BeautifulSoup(html_navios_porto, 'lxml')
            # Tag <table> dos navios.
            table = soup.find('table', class_='table table-hover text-left')

            # Percorrer todas as linhas da tabela.
            # A primeira linha é o cabeçalho, então iremos pulá-la.
            linhas = table.find_all('tr')

            primeira_linha_dados = True
            rowspan_porto_origem = False
            rowspan_eta_calculado = False

            for linha in linhas[1:]:

                # Cada linha contém uma lista de células com os valores de interesse.
                celulas = linha.find_all('td')

                # Pular linha de propagada que contém apenas uma célula <td>.
                if len(celulas) == 1: continue

                # Issue #9.
                # A primeira linha de dados tem a segunda célula com rowspan.
                # As demais linhas não tem essa célula, então os ínidices das células
                # precisam ser ajustados.
                idx_porto_origem = 1
                idx_nome_navio = 2
                idx_eta_informado = 3
                idx_eta_calculado = 4
                idx_chegada_atual = 5
                idx_posicao_navio = 6
                if primeira_linha_dados:
                    col = celulas[idx_porto_origem]
                    if col.has_attr('rowspan'):
                        rowspan_porto_origem = True
                    col = celulas[4]
                    if col.has_attr('rowspan'):
                        rowspan_eta_calculado = True
                    primeira_linha_dados = False
                else:
                    if rowspan_porto_origem:
                        idx_porto_origem = None
                        idx_nome_navio -= 1
                        idx_eta_informado -= 1
                        idx_eta_calculado -= 1
                        idx_chegada_atual -= 1
                        idx_posicao_navio -= 1
                    if rowspan_eta_calculado:
                        idx_eta_calculado = None
                        idx_chegada_atual -= 1
                        idx_posicao_navio -= 1


                # Coluna nome do porto de origem.
                nome_porto_origem = None
                if idx_porto_origem:
                    col = celulas[idx_porto_origem]
                    nome_porto_origem = col.text.strip()

                # Coluna nome  do navio.
                col = celulas[idx_nome_navio]
                nome_navio = col.a.text.strip()
                link_navio = col.a['href'].strip()
                link_icone_tipo_navio = None
                if col.img:
                    link_icone_tipo_navio = col.img['src']

                    # Se não for do tipo tanker (vi8.png), pula para próximo navio.
                    if link_icone_tipo_navio.find('vessel_types/vi8.png') == -1:
                        continue
                # Se não contiver imagem do tipo, pula para próximo navio.
                else:
                    continue

                # Coluna ETA Informado.
                eta_informado = None
                col = celulas[idx_eta_informado]
                if col.span:
                    if col.span.has_attr('data-time'):
                        valor_data = col.span['data-time']
                        if valor_data:
                            eta_informado = converte_data(int(valor_data))

                # Coluna ETA Calculado.
                eta_calculado = None
                if idx_eta_calculado:
                    col = celulas[idx_eta_calculado]
                    if col.span:
                        if col.span.has_attr('data-time'):
                            valor_data = col.span['data-time']
                            if valor_data:
                                eta_calculado = converte_data(int(valor_data))

                # Coluna Chegada Atual.
                data_chegada = None
                col = celulas[idx_chegada_atual]
                if col.span:
                    if col.span.has_attr('data-time'):
                        valor_data = col.span['data-time']
                        if valor_data:
                            data_chegada = converte_data(int(valor_data))

                # Link posição do navio
                link_posicao_navio = None
                col = celulas[idx_posicao_navio]
                if col.a:
                    link_posicao_navio = col.a['href']


                # Armazena os dados de cada navio na tabela de navios.
                dados = [nome_porto, nome_porto_origem,nome_navio, eta_informado, eta_calculado, data_chegada, link_navio, link_icone_tipo_navio, link_posicao_navio, data_coleta()]
                tabela_chegadas_esperadas.append(dados)

            # Não há próxima página?
            next_disabled = soup.find('span', class_='next disabled')
            if next_disabled:
                logger.info('Fim da captura de chegadas esperadas para o ' \
                    'porto {}.'.format(nome_porto))
                break
            elif soup.find('span', class_='next'):
                next_page = soup.find('span', class_='next')
                url_chegadas_esperadas = URL_BASE + next_page.a['href']
            else:
                logger.info('Fim da captura de chegadas esperadas para o ' \
                    'porto {}.'.format(nome_porto))
                break

    cabecalho = ['Porto', 'PortoOrigem','Navio','ETAInformado','ETACalculado', 'DataChegada',
        'LinkNavio','LinkIconeTipoNavio', 'LinkPosicaoNavio', 'DataColeta']
    df = pd.DataFrame(tabela_chegadas_esperadas, columns=cabecalho)

    # Pegar latitude e longitude a partir do link da posição.
    df_latlong = df.LinkPosicaoNavio.str.extract('centerx:(?P<Longitude>-?\d{,3}\.?\d*)/centery:-?(?P<Latitude>\d{,3}\.?\d*)', expand=True)

    # Issue #5
    df_latlong['Latitude'] = df_latlong.Latitude.str.replace('.', ',')
    df_latlong['Longitude'] = df_latlong.Longitude.str.replace('.', ',')
    df = df.join(df_latlong)

    caminho_arquivo = Path(arquivo_csv)
    cria_pasta(caminho_arquivo)
    salva_dataframe_csv(df, caminho_arquivo.as_posix())


def __configurar_log():
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler("marinetraffic.log")
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    logger.setLevel(logging.INFO)


if __name__ =='__main__':
    __configurar_log()

    proxies = {
            'http': 'http://127.0.0.1:53128',
            'https': 'http://127.0.0.1:53128',
        }
    proxies = None
    #crawl_navios_interesse(proxy = proxies)
    #crawl_portos_brasil(proxy = proxies)
    #crawl_navios_em_portos(proxy = proxies)
    crawl_chegadas_esperadas(proxy = proxies)
