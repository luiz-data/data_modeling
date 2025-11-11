import os
# import pandas as pd # Não necessário para este script específico
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
# from datetime import datetime # Não necessário para este script específico

# -------------------------------
# Variáveis de Configuração
# -------------------------------
# Carrega as variáveis de ambiente a partir do arquivo .env
# O arquivo .env está um nível acima da pasta 'scripts'
path_to_env = "../.env" 
load_dotenv(dotenv_path=path_to_env, override=True)

# -------------------------------
# Funções de Conexão e Utilitários
# -------------------------------
def get_engine(echo=False):
    """
    Cria e retorna o engine de conexão com o banco de dados PostgreSQL.
    
    Args:
        echo (bool): Se True, o SQLAlchemy logará todas as instruções SQL.

    Returns:
        sqlalchemy.engine.Engine: O engine de conexão, ou None em caso de erro.
    """
    try:
        pg_user = os.getenv('PG_USER')
        pg_pass = os.getenv('PG_PASS')
        pg_host = os.getenv('PG_HOST')
        pg_port = os.getenv('PG_PORT')
        pg_db = os.getenv('PG_DB')

        # Verifica se todas as variáveis de ambiente necessárias foram definidas
        if not all([pg_user, pg_pass, pg_host, pg_port, pg_db]):
            raise ValueError("Uma ou mais variáveis de ambiente do PostgreSQL não foram definidas. "
                             "Verifique seu arquivo .env.")

        url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(url, pool_pre_ping=True, echo=echo)

        # Testa a conexão executando uma query simples
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Conexão com o banco de dados PostgreSQL estabelecida com sucesso.")
        return engine
        
    except ValueError as ve:
        print(f"Erro de configuração: {ve}")
        return None
    except SQLAlchemyError as sa_e:
        print(f"Erro de SQLAlchemy ao conectar ou testar o banco de dados: {sa_e}")
        print("Verifique as credenciais do banco de dados, o host, a porta e se o banco de dados existe.")
        return None
    except Exception as e:
        print(f"Erro inesperado ao criar o engine de conexão: {e}")
        return None


# -------------------------------
# Função Principal de Carregamento
# -------------------------------
def load_bronze():
    """
    Carrega a camada Bronze, criando tabelas e inserindo dados no PostgreSQL.
    """
    engine = get_engine(echo=False) # Defina echo=True para ver os comandos SQL no console
    if engine is None:
        print("Não foi possível criar conexão com o banco de dados. Abortando a carga da camada Bronze.")
        return

    # O caminho para 'oltp_queries' é relativo ao diretório onde o script é executado.
    # Se o script está em 'scripts/' e 'oltp_queries' está na raiz do projeto,
    # o caminho correto para acessar 'oltp_queries' é '../oltp_queries'
    queries_path = "../oltp_queries" 

    try:
        with engine.connect() as conn:
            # --- Executa create_table.sql ---
            create_table_sql_path = os.path.join(queries_path, "create_table.sql")
            
            # Verifica se o arquivo SQL de criação de tabelas existe
            if not os.path.exists(create_table_sql_path):
                raise FileNotFoundError(f"Arquivo SQL de criação de tabelas não encontrado: {create_table_sql_path}")
            
            print(f"Executando script de criação de tabelas: {create_table_sql_path}")
            with open(create_table_sql_path, "r", encoding="utf-8") as f:
                create_sql = f.read()
                conn.execute(text(create_sql))
                conn.commit() # Confirma as alterações no banco de dados
                print("Tabelas criadas ou verificadas com sucesso na camada Bronze.")

            # --- Executa insert_into.sql ---
            insert_into_sql_path = os.path.join(queries_path, "insert_into.sql")
            
            # Verifica se o arquivo SQL de inserção de dados existe
            if not os.path.exists(insert_into_sql_path):
                raise FileNotFoundError(f"Arquivo SQL de inserção de dados não encontrado: {insert_into_sql_path}")
            
            print(f"Executando script de inserção de dados: {insert_into_sql_path}")
            with open(insert_into_sql_path, "r", encoding="utf-8") as f:
                insert_sql = f.read()
                conn.execute(text(insert_sql))
                conn.commit() # Confirma as alterações no banco de dados
                print("Dados inseridos com sucesso na camada Bronze.")

        print("\nCarga da camada Bronze concluída com sucesso.")

    except FileNotFoundError as fnfe:
        print(f"Erro: {fnfe}")
    except SQLAlchemyError as e:
        print(f"Erro ao executar as queries SQL: {e}")
        print("Verifique a sintaxe SQL nos arquivos e as dependências entre as tabelas (chaves estrangeiras).")
        # conn.rollback() # Se estivéssemos em uma transação maior que abrangia várias execuções, um rollback seria útil aqui.
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a carga da camada Bronze: {e}")

if __name__ == "__main__":
    load_bronze()