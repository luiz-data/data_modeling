import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, types
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import pytz

# -------------------------------
# Variáveis e Funções de Conexão
# -------------------------------
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
            raise ValueError("PostgreSQL env vars not set.")
        url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(url, pool_pre_ping=True, echo=echo)
        with engine.connect() as conn: conn.execute(text("SELECT 1"))
        print("DB connection successful.")
        return engine
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None

# -------------------------------
# Funções Auxiliares
# -------------------------------

def _get_unknown_value(dtype):
    """Retorna um valor 'Unknown' apropriado para o tipo de dado."""
    if pd.api.types.is_datetime64_any_dtype(dtype): return pd.NaT
    if pd.api.types.is_numeric_dtype(dtype): return pd.NA # Usa pd.NA para numéricos com nulos
    if pd.api.types.is_bool_dtype(dtype): return False
    return 'Unknown'

def add_unknown_member(df, sk_col, natural_key_col, unknown_natural_key='UNKNOWN', unknown_sk=-1, **defaults):
    """Adiciona um membro 'Unknown' a uma dimensão."""
    unknown_row = {sk_col: unknown_sk, natural_key_col: unknown_natural_key}
    for col in df.columns:
        if col not in unknown_row:
            unknown_row[col] = defaults.get(col, _get_unknown_value(df[col].dtype))
    
    # Converte unknown_row para DataFrame e garante que os tipos sejam compatíveis antes de concatenar
    # Isso é importante para colunas como 'age' que são Int64 e aceitam pd.NA
    unknown_df = pd.DataFrame([unknown_row])
    for col, dtype in df.dtypes.items():
        if col in unknown_df.columns:
            if pd.api.types.is_integer_dtype(dtype) and not pd.api.types.is_extension_array_dtype(dtype):
                unknown_df[col] = pd.to_numeric(unknown_df[col], errors='coerce').astype('Int64')
            elif pd.api.types.is_float_dtype(dtype):
                unknown_df[col] = pd.to_numeric(unknown_df[col], errors='coerce')
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                unknown_df[col] = pd.to_datetime(unknown_df[col], errors='coerce')
            # Outros tipos devem ser tratados conforme necessário, mas string e bool geralmente são ok.
    
    return pd.concat([unknown_df, df], ignore_index=True)

def add_audit_columns(df):
    """Adiciona colunas de auditoria dw_gold_created_at e dw_gold_updated_at."""
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    return df.assign(dw_gold_created_at=now_utc, dw_gold_updated_at=now_utc)

def merge_and_fill_sk(df, dim_df, on_col_key, sk_col_name, original_sk_col_to_int=None):
    """
    Realiza o merge de um DataFrame de fatos com uma dimensão de data para adicionar SKs,
    e preenche valores nulos com -1.
    
    Args:
        df (pd.DataFrame): DataFrame de fatos.
        dim_df (pd.DataFrame): DataFrame de dimensão (espera 'date_key' e 'date_sk').
        on_col_key (str): Nome da coluna no DataFrame de fatos a ser usada para o merge (contém a data formatada).
        sk_col_name (str): Nome da nova coluna SK a ser criada no DataFrame de fatos.
        original_sk_col_to_int (str, optional): Se fornecido, converte esta coluna SK existente para int, 
                                                tratando nulos como -1 ANTES do merge da data.
    Returns:
        pd.DataFrame: DataFrame de fatos com a nova coluna SK e SKs existentes tratadas.
    """
    # Garante que as SKs existentes sejam tratadas antes de adicionar as de data
    if original_sk_col_to_int:
        df[original_sk_col_to_int] = df[original_sk_col_to_int].fillna(-1).astype('Int64') # Int64 para nulos

    merged = pd.merge(df, dim_df[['date_key', 'date_sk']], 
                      left_on=on_col_key, right_on='date_key', how='left') \
               .rename(columns={'date_sk': sk_col_name}) \
               .drop(columns=['date_key'])
    
    # Preenche a nova SK de data com -1 para datas não encontradas
    merged[sk_col_name] = merged[sk_col_name].fillna(-1).astype('Int64') # Int64 para nulos
    
    return merged

# -------------------------------
# Funções de Construção da Camada Gold (Star Schema)
# -------------------------------

def create_dim_date(start_date, end_date):
    """Cria a dimensão de data."""
    dates = pd.to_datetime(pd.date_range(start=start_date, end=end_date, freq='D'))
    dim_date_df = pd.DataFrame({
        'date_sk': np.arange(1, len(dates) + 1),
        'date_key': dates.strftime('%Y-%m-%d'),
        'year': dates.year, 'quarter': dates.quarter, 'month': dates.month, 'day': dates.day,
        'day_of_week': dates.dayofweek, 'day_name': dates.day_name(), 'month_name': dates.month_name(),
        'week_of_year': dates.isocalendar().week.astype(int), 'is_weekend': (dates.dayofweek >= 5).astype(bool)
    })
    return dim_date_df.pipe(add_unknown_member, 'date_sk', 'date_key', '9999-12-31', 
                            year=9999, quarter=99, month=99, day=99, day_of_week=9, 
                            day_name='Unknown', month_name='Unknown', week_of_year=99, is_weekend=False) \
                      .pipe(add_audit_columns)

def build_dim_patient(silver_patients_df):
    """Constrói a dimensão de paciente."""
    # Garante que age seja Int64 antes de passar para add_unknown_member
    silver_patients_df['age'] = silver_patients_df['age'].astype('Int64')
    return silver_patients_df.rename(columns={'patient_id': 'patient_natural_key'}) \
                             .pipe(add_unknown_member, 'patient_sk', 'patient_natural_key', 
                                   full_name='Unknown Patient', date_of_birth=pd.NaT, age=pd.NA, age_group='Unknown') \
                             .pipe(add_audit_columns)

def build_dim_provider(silver_providers_df):
    """Constrói a dimensão de provedor."""
    return silver_providers_df.rename(columns={'provider_id': 'provider_natural_key'}) \
                              .pipe(add_unknown_member, 'provider_sk', 'provider_natural_key', provider_name='Unknown Provider') \
                              .pipe(add_audit_columns)

def build_dim_payer(silver_payers_df):
    """Constrói a dimensão de pagador."""
    return silver_payers_df.rename(columns={'payer_id': 'payer_natural_key'}) \
                           .pipe(add_unknown_member, 'payer_sk', 'payer_natural_key', payer_name='Unknown Payer') \
                           .pipe(add_audit_columns)

def build_generic_dimension(source_df, natural_key_col_source, sk_col_name, natural_key_col_name, description_col_name=None):
    """Constrói uma dimensão genérica a partir de uma coluna de origem."""
    dim_df = source_df[[natural_key_col_source]].drop_duplicates().dropna().reset_index(drop=True) \
                   .rename(columns={natural_key_col_source: natural_key_col_name}) \
                   .assign(**{sk_col_name: lambda df: np.arange(1, len(df) + 1)})
    if description_col_name:
        dim_df[description_col_name] = dim_df[natural_key_col_name].apply(lambda x: f"{natural_key_col_name.replace('_', ' ').title()} {x}")
    return dim_df.pipe(add_unknown_member, sk_col_name, natural_key_col_name, 
                       **({description_col_name: f"Unknown {natural_key_col_name.replace('_', ' ').title()}"} if description_col_name else {})) \
                 .pipe(add_audit_columns)

def build_dim_procedure(silver_claims_transactions_df):
    """Constrói a dimensão de procedimento."""
    return build_generic_dimension(silver_claims_transactions_df, 'procedure_code', 'procedure_sk', 'procedure_code', 'procedure_description')

def build_dim_encounter_type(silver_encounters_df):
    """Constrói a dimensão de tipo de encontro."""
    return build_generic_dimension(silver_encounters_df, 'encounter_type', 'encounter_type_sk', 'encounter_type')

def build_fact_claims(silver_claims_df, dim_date_df):
    """Constrói a tabela de fatos de claims."""
    # Garante que as SKs existentes sejam Int64 para aceitar pd.NA antes de usar merge_and_fill_sk
    silver_claims_df['patient_sk'] = silver_claims_df['patient_sk'].fillna(-1).astype('Int64')
    silver_claims_df['provider_sk'] = silver_claims_df['provider_sk'].fillna(-1).astype('Int64')

    return silver_claims_df.assign(
        claim_start_date_key=lambda df: df['claim_start_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31'),
        claim_end_date_key=lambda df: df['claim_end_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31')
    ).pipe(merge_and_fill_sk, dim_date_df, 'claim_start_date_key', 'claim_start_date_sk') \
     .pipe(merge_and_fill_sk, dim_date_df, 'claim_end_date_key', 'claim_end_date_sk') \
     .pipe(add_audit_columns) \
     [['claim_id', 'patient_sk', 'provider_sk', 'claim_start_date_sk', 'claim_end_date_sk', 'total_outstanding', 'dw_gold_created_at', 'dw_gold_updated_at']]

def build_fact_encounters(silver_encounters_df, dim_date_df, dim_encounter_type_df):
    """Constrói a tabela de fatos de encontros."""
    # Garante que as SKs existentes sejam Int64 para aceitar pd.NA antes de usar merge_and_fill_sk
    silver_encounters_df['patient_sk'] = silver_encounters_df['patient_sk'].fillna(-1).astype('Int64')
    silver_encounters_df['provider_sk'] = silver_encounters_df['provider_sk'].fillna(-1).astype('Int64')
    silver_encounters_df['payer_sk'] = silver_encounters_df['payer_sk'].fillna(-1).astype('Int64')
    
    return silver_encounters_df.assign(
        encounter_date_key=lambda df: df['encounter_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31'),
        discharge_date_key=lambda df: df['discharge_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31')
    ).pipe(merge_and_fill_sk, dim_date_df, 'encounter_date_key', 'encounter_date_sk') \
     .pipe(merge_and_fill_sk, dim_date_df, 'discharge_date_key', 'discharge_date_sk') \
     .merge(dim_encounter_type_df[['encounter_type', 'encounter_type_sk']], on='encounter_type', how='left') \
     .assign(encounter_type_sk=lambda df: df['encounter_type_sk'].fillna(-1).astype('Int64')) \
     .pipe(add_audit_columns) \
     [['encounter_id', 'patient_sk', 'provider_sk', 'payer_sk', 'encounter_type_sk', 'encounter_date_sk', 
       'discharge_date_sk', 'total_claim_cost', 'payer_coverage', 'length_of_stay_days', 'dw_gold_created_at', 'dw_gold_updated_at']]

def build_fact_claim_transactions(silver_claims_transactions_df, dim_date_df, dim_procedure_df):
    """Constrói a tabela de fatos de transações de claims."""
    # Garante que as SKs existentes sejam Int64 para aceitar pd.NA antes de usar merge_and_fill_sk
    silver_claims_transactions_df['patient_sk'] = silver_claims_transactions_df['patient_sk'].fillna(-1).astype('Int64')
    silver_claims_transactions_df['provider_sk'] = silver_claims_transactions_df['provider_sk'].fillna(-1).astype('Int64')

    return silver_claims_transactions_df.assign(
        transaction_date_key=lambda df: df['transaction_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31')
    ).pipe(merge_and_fill_sk, dim_date_df, 'transaction_date_key', 'transaction_date_sk') \
     .merge(dim_procedure_df[['procedure_code', 'procedure_sk']], on='procedure_code', how='left') \
     .assign(procedure_sk=lambda df: df['procedure_sk'].fillna(-1).astype('Int64')) \
     .pipe(add_audit_columns) \
     [['transaction_id', 'claim_id', 'patient_sk', 'provider_sk', 'transaction_date_sk', 'procedure_sk', 
       'transaction_amount', 'dw_gold_created_at', 'dw_gold_updated_at']]

# -------------------------------
# Configurações de Schemas para Carregamento
# -------------------------------

DIMENSION_CONFIGS = {
    "gold_dim_date": {
        "builder": create_dim_date,
        "params": lambda silver_data, min_date, max_date: [min_date, max_date],
        "dtypes": {
            'date_sk': types.BigInteger, 'date_key': types.String(10), 
            'year': types.SmallInteger, 'quarter': types.SmallInteger, 
            'month': types.SmallInteger, 'day': types.SmallInteger,
            'day_of_week': types.SmallInteger, 'day_name': types.String(10), 
            'month_name': types.String(10), 'week_of_year': types.SmallInteger, 
            'is_weekend': types.Boolean, 'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
    "gold_dim_patient": {
        "builder": build_dim_patient,
        "params": lambda silver_data, *args: [silver_data["silver_dim_patient"]],
        "dtypes": {
            'patient_sk': types.BigInteger, 'patient_natural_key': types.String(50),
            'full_name': types.String(255), 'date_of_birth': types.Date,
            'age': types.SmallInteger, 'age_group': types.String(20),
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
    "gold_dim_provider": {
        "builder": build_dim_provider,
        "params": lambda silver_data, *args: [silver_data["silver_dim_provider"]],
        "dtypes": {
            'provider_sk': types.BigInteger, 'provider_natural_key': types.String(50),
            'provider_name': types.String(255),
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
    "gold_dim_payer": {
        "builder": build_dim_payer,
        "params": lambda silver_data, *args: [silver_data["silver_dim_payer"]],
        "dtypes": {
            'payer_sk': types.BigInteger, 'payer_natural_key': types.String(50),
            'payer_name': types.String(255),
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
    "gold_dim_procedure": {
        "builder": build_dim_procedure,
        "params": lambda silver_data, *args: [silver_data["silver_fact_claim_transaction"]],
        "dtypes": {
            'procedure_sk': types.BigInteger, 'procedure_code': types.String(50),
            'procedure_description': types.String(255),
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
    "gold_dim_encounter_type": {
        "builder": build_dim_encounter_type,
        "params": lambda silver_data, *args: [silver_data["silver_fact_encounter"]],
        "dtypes": {
            'encounter_type_sk': types.BigInteger, 'encounter_type': types.String(50),
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
}

FACT_CONFIGS = {
    "gold_fact_claims": {
        "builder": build_fact_claims,
        "params": lambda silver_data, gold_data, *args: [silver_data["silver_fact_claim"], gold_data["gold_dim_date"]],
        "dtypes": {
            'claim_id': types.String(50), 'patient_sk': types.BigInteger, 
            'provider_sk': types.BigInteger, 'claim_start_date_sk': types.BigInteger, 
            'claim_end_date_sk': types.BigInteger, 'total_outstanding': types.Numeric(10, 2),
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
    "gold_fact_encounters": {
        "builder": build_fact_encounters,
        "params": lambda silver_data, gold_data, *args: [silver_data["silver_fact_encounter"], gold_data["gold_dim_date"], gold_data["gold_dim_encounter_type"]],
        "dtypes": {
            'encounter_id': types.String(50), 'patient_sk': types.BigInteger, 
            'provider_sk': types.BigInteger, 'payer_sk': types.BigInteger, 
            'encounter_type_sk': types.BigInteger, 'encounter_date_sk': types.BigInteger, 
            'discharge_date_sk': types.BigInteger, 'total_claim_cost': types.Numeric(10, 2), 
            'payer_coverage': types.Numeric(10, 2), # <-- CORREÇÃO AQUI: Aumentado para Numeric(10, 2)
            'length_of_stay_days': types.SmallInteger,
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
    "gold_fact_claim_transactions": {
        "builder": build_fact_claim_transactions,
        "params": lambda silver_data, gold_data, *args: [silver_data["silver_fact_claim_transaction"], gold_data["gold_dim_date"], gold_data["gold_dim_procedure"]],
        "dtypes": {
            'transaction_id': types.String(50), 'claim_id': types.String(50), 
            'patient_sk': types.BigInteger, 'provider_sk': types.BigInteger,
            'transaction_date_sk': types.BigInteger, 'procedure_sk': types.BigInteger, 
            'transaction_amount': types.Numeric(10, 2),
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            'dw_gold_updated_at': types.TIMESTAMP(timezone=True)
        }
    },
}

# -------------------------------
# Função Principal de Carregamento da Camada Gold
# -------------------------------
def load_gold():
    engine = get_engine()
    if engine is None: return

    silver_data = {}
    gold_data = {}

    try:
        print("Reading Silver layer data...")
        # CORREÇÃO ANTERIOR APLICADA: Removido o argumento 'dtype' com tipos SQLAlchemy
        silver_data["silver_dim_patient"] = pd.read_sql("SELECT * FROM silver_dim_patient", engine)
        # Opcional: Conversões explícitas se o Pandas não inferir perfeitamente
        silver_data["silver_dim_patient"]['date_of_birth'] = pd.to_datetime(silver_data["silver_dim_patient"]['date_of_birth'], errors='coerce').dt.date
        silver_data["silver_dim_patient"]['age'] = silver_data["silver_dim_patient"]['age'].astype('Int64') # Para suportar NA

        silver_data["silver_dim_payer"] = pd.read_sql("SELECT * FROM silver_dim_payer", engine)
        silver_data["silver_dim_provider"] = pd.read_sql("SELECT * FROM silver_dim_provider", engine)
        
        silver_data["silver_fact_claim"] = pd.read_sql("SELECT * FROM silver_fact_claim", engine, parse_dates=['claim_start_date', 'claim_end_date'])
        silver_data["silver_fact_claim_transaction"] = pd.read_sql("SELECT * FROM silver_fact_claim_transaction", engine, parse_dates=['transaction_date'])
        silver_data["silver_fact_encounter"] = pd.read_sql("SELECT * FROM silver_fact_encounter", engine, parse_dates=['encounter_date', 'discharge_date'])
        
        print("Silver layer extraction complete.")
    except Exception as e:
        print(f"Error extracting Silver layer data: {e}"); return

    try:
        print("Calculating date range for date dimension...")
        all_dates = pd.concat([
            silver_data["silver_fact_claim"]['claim_start_date'], silver_data["silver_fact_claim"]['claim_end_date'],
            silver_data["silver_fact_claim_transaction"]['transaction_date'],
            silver_data["silver_fact_encounter"]['encounter_date'], silver_data["silver_fact_encounter"]['discharge_date']
        ]).dropna().drop_duplicates()
        min_date = all_dates.min() if not all_dates.empty else pd.Timestamp('2020-01-01')
        max_date = all_dates.max() if not all_dates.empty else pd.Timestamp.today() + pd.DateOffset(years=1)
        print(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")

        print("Building Gold layer Dimensions...")
        for table_name, config in DIMENSION_CONFIGS.items():
            print(f"  Building {table_name}...")
            params = config["params"](silver_data, min_date, max_date)
            gold_data[table_name] = config["builder"](*params)
            # Ao carregar para o banco de dados, usamos os dtypes SQLAlchemy
            gold_data[table_name].to_sql(table_name, engine, if_exists="replace", index=False, dtype=config["dtypes"])
        print("Gold layer Dimensions loaded.")

        print("Building Gold layer Fact Tables...")
        for table_name, config in FACT_CONFIGS.items():
            print(f"  Building {table_name}...")
            params = config["params"](silver_data, gold_data, min_date, max_date)
            gold_data[table_name] = config["builder"](*params)
            # Ao carregar para o banco de dados, usamos os dtypes SQLAlchemy
            gold_data[table_name].to_sql(table_name, engine, if_exists="replace", index=False, dtype=config["dtypes"])
        print("Gold layer Fact Tables loaded.")

        print("\nGold layer loaded successfully (Star Schema built).")
    except Exception as e:
        print(f"Error during Gold layer build or load: {e}")

if __name__ == "__main__":
    load_gold()