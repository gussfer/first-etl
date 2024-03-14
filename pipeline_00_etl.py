import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()


# EXTRACT 
# Função para baixar uma pasta do Google Drive
def baixar_pasta_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)




# Função para listar arquivos CSV no diretório especificado
def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    # print(arquivos_csv)
    return arquivos_csv

# Função para ler um arquivo CSV e retornar um DataFrame duckdb
def ler_csv(caminho_do_arquivo):
    df_duckdb = duckdb.read_csv(caminho_do_arquivo)
    return df_duckdb


# TRANSFORM
# Função para adicionar uma coluna de total de vendas
def transformar(df):
    # Executa a consulta SQL que inclui a nova coluna, operando sobre a tabela virtual
    df_transformado = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    # print(df_transformado)
    # Remove o registro da tabela virtual para limpeza
    return df_transformado

# Função para converter o Duckdb em Pandas e salvar o DataFrame no PostgreSQL
def salvar_no_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")  # Ex: 'postgresql://user:password@localhost:5432/database_name'
    engine = create_engine(DATABASE_URL)
    # Salvar o DataFrame no PostgreSQL
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

if __name__ == '__main__':
    url_pasta = 'https://drive.google.com/drive/folders/1v-JW6kUM1NM8t62Y_Z5-yOTBioMBe04C'
    diretorio_local = './pasta_gdown'
    # baixar_pasta_google_drive(url_pasta, diretorio_local)
    arquivos = listar_arquivos_csv(diretorio_local)

    for caminho_do_arquivo in arquivos:
        duck_db_df = ler_csv(caminho_do_arquivo)
        pandas_df_transformado = transformar(duck_db_df)
        salvar_no_postgres(pandas_df_transformado, "total_vendas")
