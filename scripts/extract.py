# Testando a extração da tabela de COVID-19 utilizando API com paginação

import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# '?is_last=True' garante que traga apenas os dados mais recentes por cidade/estado
api = os.getenv('API_COVID_URL')
token = os.getenv('BRASIL_IO_TOKEN')

headers = {
    'Authorization': f'Token {token}'
}

def extract_file():
    url = api  # começa com a URL inicial da API
    all_data = []

    while url:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            results = data['results']
            df_temp = pd.json_normalize(results)
            all_data.append(df_temp)
            print(f'Dados extraídos de: {url}')
            url = data['next']  # próxima página, ou None se acabou
        else:
            print(f'Erro ao acessar {url}: {response.status_code}')
            break

    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        df.to_csv(
            r'C:\Users\lcfer\OneDrive\Documentos\02_Work\00_Projects\01_Data Engineering\01_ETL com Python e Azure\data\raw\dados.csv',
            index=False
        )
        print('Todos os dados foram extraídos com sucesso.')
        return df
    else:
        print('Nenhum dado foi extraído.')
        return None

if __name__ == '__main__':
    extract_file()