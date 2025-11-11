import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import pytz # Para lidar com fusos horários se necessário em timestamps

# -------------------------------
# Variáveis e Funções de Conexão
# -------------------------------
# O caminho para o arquivo .env deve ser relativo ao diretório de execução do script.
# Se este script está em 'scripts/' e o .env está na raiz do projeto, '../.env' está correto.
path_to_env = "../.env"
load_dotenv(dotenv_path=path_to_env, override=True)

def get_engine(echo=False):
    """
    Cria e retorna o engine de conexão com o banco de dados PostgreSQL.
    """
    try:
        pg_user = os.getenv('PG_USER')
        pg_pass = os.getenv('PG_PASS')
        pg_host = os.getenv('PG_HOST')
        pg_port = os.getenv('PG_PORT')
        pg_db = os.getenv('PG_DB')

        if not all([pg_user, pg_pass, pg_host, pg_port, pg_db]):
            raise ValueError("Uma ou mais variáveis de ambiente do PostgreSQL não foram definidas. "
                             "Verifique seu arquivo .env.")

        url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(url, pool_pre_ping=True, echo=echo)

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
# Funções de Transformação Gerais
# -------------------------------
def calculate_age(date_of_birth, as_of_date=None):
    """
    Calcula a idade de uma série de datas de nascimento de forma vetorizada.
    Retorna pd.NA para datas de nascimento inválidas ou futuras.
    """
    if as_of_date is None:
        as_of_date = datetime.now(pytz.utc).date() # Usar UTC para consistência
    else:
        as_of_date = pd.to_datetime(as_of_date).date()

    # Converte a série de datas de nascimento para datetime e extrai a parte da data
    dob_series = pd.to_datetime(date_of_birth, errors='coerce').dt.date

    # Inicializa a série de idades com pd.NA
    age = pd.Series(pd.NA, index=dob_series.index, dtype='Int64')

    # Máscara para filtrar apenas as datas de nascimento válidas
    valid_dob_mask = dob_series.notna()
    
    # Extrai as datas de nascimento válidas
    valid_dobs = dob_series[valid_dob_mask]

    # Calcula a idade inicial baseada apenas no ano
    age.loc[valid_dob_mask] = as_of_date.year - valid_dobs.apply(lambda x: x.year)

    # Ajusta a idade se o aniversário ainda não ocorreu no ano atual
    # Cria uma máscara booleana para as datas que precisam de ajuste
    # A comparação de tuplas para o mês/dia é feita de forma vetorizada
    needs_adjustment_mask = (
        (as_of_date.month < valid_dobs.apply(lambda x: x.month)) |
        ((as_of_date.month == valid_dobs.apply(lambda x: x.month)) & (as_of_date.day < valid_dobs.apply(lambda x: x.day)))
    )
    
    # Aplica o ajuste de -1 na idade para os casos necessários
    age.loc[valid_dob_mask][needs_adjustment_mask] -= 1

    # Trata idades negativas (datas de nascimento no futuro)
    age[age < 0] = pd.NA
    
    return age.astype('Int64') # Int64 permite valores nulos

def derive_age_group(age_series):
    """
    Deriva a faixa etária a partir de uma série de idades.
    """
    bins = [-1, 12, 17, 25, 35, 45, 60, 75, float('inf')]
    labels = ['0-12', '13-17', '18-25', '26-35', '36-45', '46-60', '61-75', '75+']

    # pd.cut lida com np.nan e pd.NA, que resultam em NaN no resultado e são substituídos.
    return pd.cut(
        age_series,
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True
    ).astype(str).replace('nan', 'Unknown')

# -------------------------------
# Funções de Transformação Específicas para cada tabela
# Orientadas a Star Schema
# -------------------------------

def transform_patients_to_silver(df):
    """
    Transforma dados de pacientes da camada Bronze para Silver,
    preparando-os para se tornarem uma dimensão de Pacientes na Gold.
    """
    df = df.copy()

    # Geração de Surrogate Key (SK) - Exemplo simples com factorize
    df['patient_sk'] = pd.factorize(df['patient_id'])[0] + 1

    # Padroniza nomes e cria full_name
    df['first_name'] = df['first_name'].str.strip().str.title().fillna('')
    df['last_name'] = df['last_name'].str.strip().str.title().fillna('')
    df['full_name'] = df.apply(
        lambda row: f"{row['first_name']} {row['last_name']}".strip() if row['first_name'] or row['last_name'] else 'Unknown Patient',
        axis=1
    )

    # Conversão para datetime e tratamento de erros
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')

    # Calcula idade e faixa etária
    df['age'] = calculate_age(df['date_of_birth'])
    df['age_group'] = derive_age_group(df['age'])

    # Adiciona campos de auditoria
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    df['dw_created_at'] = now_utc
    df['dw_updated_at'] = now_utc

    # Seleciona e organiza colunas para a tabela Silver Dim Patient
    df_silver = df[['patient_sk', 'patient_id', 'full_name', 'date_of_birth', 'age', 'age_group', 'dw_created_at', 'dw_updated_at']].copy()
    return df_silver

def transform_payers_to_silver(df):
    """
    Transforma dados de payers da camada Bronze para Silver,
    preparando-os para se tornarem uma dimensão de Payers na Gold.
    """
    df = df.copy()

    # Geração de Surrogate Key (SK)
    df['payer_sk'] = pd.factorize(df['payer_id'])[0] + 1

    # Padroniza payer_name
    df['payer_name'] = df['payer_name'].str.strip().str.title().fillna('Unknown Payer')

    # Adiciona campos de auditoria
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    df['dw_created_at'] = now_utc
    df['dw_updated_at'] = now_utc

    # Seleciona e organiza colunas para a tabela Silver Dim Payer
    df_silver = df[['payer_sk', 'payer_id', 'payer_name', 'dw_created_at', 'dw_updated_at']].copy()
    return df_silver

def transform_providers_to_silver(bronze_claims_df, bronze_encounters_df):
    """
    Identifica provedores únicos das tabelas bronze_claims e bronze_encounters,
    limpa e padroniza, preparando-os para se tornarem uma dimensão de Providers na Gold.
    """
    # Coleta todos os provider_ids únicos
    providers_claims = bronze_claims_df[['provider_id']].drop_duplicates().dropna()
    providers_encounters = bronze_encounters_df[['provider_id']].drop_duplicates().dropna()

    # Combina e obtém provedores únicos
    all_providers = pd.concat([providers_claims, providers_encounters]).drop_duplicates().reset_index(drop=True)

    # Cria uma SK para cada provider_id
    all_providers['provider_sk'] = pd.factorize(all_providers['provider_id'])[0] + 1

    # Adiciona um nome genérico ou um placeholder se não houver um nome na Bronze
    all_providers['provider_name'] = "Provider " + all_providers['provider_id'].astype(str)

    # Adiciona campos de auditoria
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    all_providers['dw_created_at'] = now_utc
    all_providers['dw_updated_at'] = now_utc

    df_silver = all_providers[['provider_sk', 'provider_id', 'provider_name', 'dw_created_at', 'dw_updated_at']].copy()
    return df_silver

def transform_claims_to_silver(bronze_claims_df, silver_patients_df, silver_providers_df):
    """
    Transforma dados de claims da camada Bronze para Silver,
    incluindo links para as novas SKs de dimensão e cálculos importantes.
    """
    df = bronze_claims_df.copy()

    # Converte datas, coerção de erros resultará em NaT
    df['claim_start_date'] = pd.to_datetime(df['claim_start_date'], errors='coerce')
    df['claim_end_date'] = pd.to_datetime(df['claim_end_date'], errors='coerce')

    # Converte valores monetários, coerção de erros resultará em NaN
    for col in ['outstanding_primary', 'outstanding_secondary', 'outstanding_patient']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0) # Assume 0 se nulo/inválido

    # Calcula total_outstanding
    df['total_outstanding'] = df['outstanding_primary'] + df['outstanding_secondary'] + df['outstanding_patient']

    # Validação de datas: se claim_start_date > claim_end_date, seta claim_end_date para null
    df.loc[df['claim_start_date'] > df['claim_end_date'], 'claim_end_date'] = pd.NaT

    # Cria links para as SKs das dimensões (patient_sk, provider_sk)
    df = pd.merge(df, silver_patients_df[['patient_id', 'patient_sk']], on='patient_id', how='left')
    df = pd.merge(df, silver_providers_df[['provider_id', 'provider_sk']], on='provider_id', how='left')

    # Adiciona campos de auditoria
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    df['dw_created_at'] = now_utc
    df['dw_updated_at'] = now_utc

    # Seleciona e organiza colunas para a tabela Silver Fact Claim
    df_silver = df[['claim_id', 'patient_sk', 'provider_sk', 'claim_start_date', 'claim_end_date',
                    'total_outstanding', 'dw_created_at', 'dw_updated_at']].copy()
    return df_silver

def transform_claims_transactions_to_silver(bronze_claims_transactions_df, silver_patients_df, silver_providers_df, silver_claims_df):
    """
    Transforma dados de transações de claims, enriquecendo-os com SKs de dimensões.
    """
    df = bronze_claims_transactions_df.copy()

    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df['transaction_amount'] = pd.to_numeric(df['transaction_amount'], errors='coerce').fillna(0)
    df['procedure_code'] = df['procedure_code'].str.strip().str.upper().fillna('UNKNOWN_CODE')

    # Cria links para as SKs das dimensões
    df = pd.merge(df, silver_patients_df[['patient_id', 'patient_sk']], on='patient_id', how='left')
    df = pd.merge(df, silver_providers_df[['provider_id', 'provider_sk']], on='provider_id', how='left')
    
    # Adiciona campos de auditoria
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    df['dw_created_at'] = now_utc
    df['dw_updated_at'] = now_utc

    # Seleciona e organiza colunas
    df_silver = df[['transaction_id', 'claim_id', 'patient_sk', 'provider_sk', 'transaction_date',
                    'transaction_amount', 'procedure_code', 'dw_created_at', 'dw_updated_at']].copy()
    return df_silver

def transform_encounters_to_silver(bronze_encounters_df, silver_patients_df, silver_providers_df, silver_payers_df):
    """
    Transforma dados de encounters da camada Bronze para Silver,
    incluindo links para as novas SKs de dimensão e cálculos importantes.
    """
    df = bronze_encounters_df.copy()

    df['encounter_date'] = pd.to_datetime(df['encounter_date'], errors='coerce')
    df['discharge_date'] = pd.to_datetime(df['discharge_date'], errors='coerce')

    df['encounter_type'] = df['encounter_type'].str.strip().str.title().fillna('Unknown Type')

    for col in ['total_claim_cost', 'payer_coverage']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Validação de datas: se encounter_date > discharge_date, seta discharge_date para null
    df.loc[df['encounter_date'] > df['discharge_date'], 'discharge_date'] = pd.NaT

    # Calcula tempo de internação/encontro em dias
    # Usa dt.days para Series, resultando em uma Series de inteiros (ou NaNs)
    df['length_of_stay_days'] = (df['discharge_date'] - df['encounter_date']).dt.days
    # Trata valores negativos ou NaN
    df['length_of_stay_days'] = df['length_of_stay_days'].apply(lambda x: max(0, x) if pd.notna(x) else pd.NA).astype('Int64')

    # Cria links para as SKs das dimensões
    df = pd.merge(df, silver_patients_df[['patient_id', 'patient_sk']], on='patient_id', how='left')
    df = pd.merge(df, silver_providers_df[['provider_id', 'provider_sk']], on='provider_id', how='left')
    df = pd.merge(df, silver_payers_df[['payer_id', 'payer_sk']], on='payer_id', how='left')

    # Adiciona campos de auditoria
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    df['dw_created_at'] = now_utc
    df['dw_updated_at'] = now_utc

    # Seleciona e organiza colunas para a tabela Silver Fact Encounter
    df_silver = df[['encounter_id', 'patient_sk', 'provider_sk', 'payer_sk',
                    'encounter_date', 'discharge_date', 'encounter_type',
                    'total_claim_cost', 'payer_coverage', 'length_of_stay_days',
                    'dw_created_at', 'dw_updated_at']].copy()
    return df_silver


# -------------------------------
# Função Principal de Carregamento da Camada Silver
# -------------------------------
def load_silver():
    engine = get_engine()
    if engine is None:
        print("Não foi possível conectar ao banco de dados. Abortando a carga da camada Silver.")
        return

    try:
        print("Lendo dados da camada Bronze...")
        # Adicionar dtypes para colunas relevantes para garantir consistência
        # 'provider_id' e 'patient_id' podem ser string para merges mais robustos
        # 'date_of_birth' pode ser lido como string e convertido depois na transformação
        patients_bronze = pd.read_sql("SELECT * FROM bronze_patients", engine)
        claims_bronze = pd.read_sql("SELECT * FROM bronze_claims", engine)
        claims_transactions_bronze = pd.read_sql("SELECT * FROM bronze_claims_transactions", engine)
        payers_bronze = pd.read_sql("SELECT * FROM bronze_payers", engine)
        encounters_bronze = pd.read_sql("SELECT * FROM bronze_encounters", engine)
        print("Extração da camada Bronze concluída.")

    except SQLAlchemyError as e:
        print(f"Erro ao extrair dados da camada Bronze: {e}")
        return
    except Exception as e:
        print(f"Erro inesperado durante a extração: {e}")
        return

    try:
        print("Aplicando transformações para a camada Silver (Dimensões primeiro)...")
        # --- Transformações para Dimensões (Entities) ---
        silver_patients = transform_patients_to_silver(patients_bronze)
        silver_payers = transform_payers_to_silver(payers_bronze)
        # Providers são inferidos de claims e encounters
        silver_providers = transform_providers_to_silver(claims_bronze, encounters_bronze)
        
        # Carregar as dimensões primeiro, pois os fatos dependem delas
        print("Carregando tabelas de Dimensão na camada Silver...")
        silver_patients.to_sql("silver_dim_patient", engine, if_exists="replace", index=False)
        silver_payers.to_sql("silver_dim_payer", engine, if_exists="replace", index=False)
        silver_providers.to_sql("silver_dim_provider", engine, if_exists="replace", index=False)
        print("Dimensões da camada Silver carregadas.")


        print("Aplicando transformações para a camada Silver (Fatos em seguida)...")
        # --- Transformações para Fatos (Eventos/Medidas) ---
        # Estes dependem das SKs das dimensões já criadas
        silver_claims = transform_claims_to_silver(claims_bronze, silver_patients, silver_providers)
        # Passar silver_claims_df para a função transform_claims_transactions_to_silver é opcional,
        # pois ela não usa claim_sk ainda. Apenas os DataFrames de dimensões são estritamente necessários para SKs.
        silver_claims_transactions = transform_claims_transactions_to_silver(claims_transactions_bronze, silver_patients, silver_providers, silver_claims)
        silver_encounters = transform_encounters_to_silver(encounters_bronze, silver_patients, silver_providers, silver_payers)

        print("Carregando tabelas de Fato na camada Silver...")
        silver_claims.to_sql("silver_fact_claim", engine, if_exists="replace", index=False)
        silver_claims_transactions.to_sql("silver_fact_claim_transaction", engine, if_exists="replace", index=False)
        silver_encounters.to_sql("silver_fact_encounter", engine, if_exists="replace", index=False)
        print("Fatos da camada Silver carregados.")

        print("\nCarga da camada Silver concluída com sucesso.")

    except Exception as e:
        print(f"Erro durante a transformação ou carga da camada Silver: {e}")

if __name__ == "__main__":
    load_silver()