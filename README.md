# crawler_marine_traffic
Crawler para o site Marine Traffic, para fins eduacionais. Não use indiscriminadamente pois pode afetar o site. Contrate a API do Marine Traffic para suas necessidades de dados.

Crawler to the Marine Traffic site, for educational purposes. Do not use it indiscriminately as it may affect the site. [Hire](http://www.marinetraffic.com/en/solutions) the Marine Traffic API for your data needs.

# Como funciona

1. Busca os portos no Brasil.
  1. Grava arquivo de portos no Brasil.
2. Busca os navios em portos do Brasil.
  1. Lê arquivo de portos de interesse.
  2. Busca navios em portos de interesse.
  3. Grava arquivo de navios em portos de interesse.
3. Busca navios com chegadas esperadas.
  1. Lê arquivo de portos de interesse.
  2. Busca navios em portos de interesse.
  3. Grava arquivo chegadas esperadas em portos de interesse.
