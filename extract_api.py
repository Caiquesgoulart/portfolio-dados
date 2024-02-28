import requests
import pandas as pd
import pandas_gbq 
import os
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError


# =============================================================================
# Configurando variáveis do bigquery e parêmetros necessários
# =============================================================================
credentials_path = 'dashboard-filmes-aec2f958f9d4.json'
project_id = 'dashboard-filmes'
dataset_id = 'dataset_filmes'
table_id = 'dashboard-filmes.dataset_filmes.tabela_filmes'

url = "https://api.themoviedb.org/3/movie/top_rated?language=en-US&page=1"


headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4NTI1MzgzMzI3YTkzNWU1NjI3MjZjM2QxOWVmZTcwNSIsInN1YiI6IjY1Y2EzMWUxOThmMWYxMDE4M2RhOTg2YSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.aHf4rl76aYZ9Rx5RJUla2oV9URl9lxWqsa2d_fg-1bc"
}


# =============================================================================
# Passo 1: Acessando a API e trazendo os dados
# =============================================================================
def get_data_from_api(url, headers):
    response = requests.get(url, headers=headers)
    data = pd.DataFrame(response.json()['results'])
    return data

# =============================================================================
# Passo 2: Criando um DataFrame e realizando tratamentos
# =============================================================================
def treat_data(dados_brutos):
    # Renomeando colunas para pt-br
    dados_brutos.rename(columns = {'adult': 'adulto', 'overview': 'descricao', 'release_date': 'data_lancamento', 'title': 'titulo', 
                        'vote_average': 'nota_media'}, inplace = True)

    # Tratando colunas data lançamento para puxar apenas o ano e renomeando ela
    dados_brutos['data_lancamento'] = pd.to_datetime(dados_brutos['data_lancamento']).dt.year
    dados_brutos.rename(columns={'data_lancamento': 'ano_lancamento'}, inplace=True)

    # Tratando coluna adulto para retornar sim/não ao invés de true/false
    dados_brutos['adulto'] = dados_brutos['adulto'].map({False: 'não', True: 'sim'})

    # Criando um dataframe apenas com as colunas que serão utilizadas                               
    dados_filmes = dados_brutos[['titulo', 'descricao', 'ano_lancamento', 'adulto', 'nota_media']]
    return dados_filmes

# =============================================================================
# Passo 3: Enviando dados para a tabela do bigquery
# =============================================================================
def load_data_to_bigquery(data, table_id, project_id, credentials_path):
    try:
        # Carrega os dados para o BigQuery
        pandas_gbq.to_gbq(data, table_id, project_id, credentials_path, if_exists = 'replace')

        # Cria um cliente BigQuery
        client = bigquery.Client()

        # Consulta a tabela para verificar se os dados foram carregados
        query_job = client.query(f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{table_id}`")
        results = query_job.result()

        # Verifica se o número de linhas é maior que zero
        if results.total_rows > 0:
            print("Dados carregados com sucesso!")
        else:
            print("Erro ao carregar os dados. Verifique os logs do BigQuery.")

    # IMPORTANTE: Esse exception retorna o seguinte erro: "Your default credentials were not found. To set up Application Default Credentials, see https://cloud.google.com/docs/authentication/external/set-up-adc for more information."
    # Não consegui resolver esse erro então deixei esse comentário para explicá-lo, o código funciona porém retorna isso
    except DefaultCredentialsError as e:
        print(f"Erro de autenticação: {e}")


# =============================================================================
# Definição da __main__
# =============================================================================
def main():
    # Acessando a API e trazendo os dados
    data = get_data_from_api(url, headers)

    # Criando um dataframe apenas com as colunas que serão utilizadas
    dados_filmes = treat_data(data)

    # Enviando dados para a tabela do bigquery
    load_data_to_bigquery(dados_filmes, table_id, project_id, credentials_path)


# =============================================================================
# Executando as funções
# =============================================================================
if __name__ == "__main__":
    main()