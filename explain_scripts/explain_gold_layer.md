# 🥇 Camada Gold - Guia de Implementação

## 📋 Índice
- [O que é a Camada Gold?](#o-que-é-a-camada-gold)
- [Silver vs Gold](#silver-vs-gold)
- [Arquitetura Star Schema](#arquitetura-star-schema)
- [Explicação do Código](#explicação-do-código)
- [Dimensões e Fatos](#dimensões-e-fatos)

---

## 🎯 O que é a Camada Gold?

A **Camada Gold** é a última camada da Arquitetura Medallion. Implementa o **Star Schema completo**, otimizado para ferramentas de BI (Power BI, Tableau) e queries analíticas de alta performance.

### Princípios
- ⭐ **Star Schema**: Dimensões + Fatos prontos para análise
- 🔑 **Surrogate Keys (SKs)**: Chaves numéricas substituem IDs naturais
- 🎭 **Unknown Members**: Registros especiais para dados faltantes (SK = -1)
- 📅 **Dimensão de Data**: Tabela de datas pré-calculada (2020-2030)
- 📊 **Otimizado para BI**: Agregações, métricas e KPIs prontos

---

## 🔄 Silver vs Gold

| Aspecto | Silver (Cleaned) | Gold (Star Schema) |
|---------|------------------|---------------------|
| **Estrutura** | Tabelas normalizadas | Star Schema desnormalizado |
| **Chaves** | SKs + Natural Keys | Apenas SKs |
| **Dimensão Data** | Colunas de data | Tabela `dim_date` completa |
| **Unknown Members** | Possíveis nulos | SK = -1 (Unknown) garantido |
| **Otimização** | Transformação | Consulta (BI) |
| **Joins** | Necessários | Pré-definidos e rápidos |

**Fluxo completo:**
```
OLTP → 🥉 Bronze (raw) → 🥈 Silver (cleaned) → 🥇 Gold (star schema) → BI Tools
```

---

## ⭐ Arquitetura Star Schema

```
                    gold_dim_date
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    gold_dim_patient     │          gold_dim_provider
         │               │               │
         └───────►gold_fact_claims◄──────┘
                         │
                         │ claim_id (relacionamento)
                         │
                         ▼
              gold_fact_claim_transactions
                         │
                         └──────► gold_dim_procedure


                    gold_dim_date
                         │
         ┌───────────────┼─────────────────────┐
         │               │                     │
    gold_dim_patient     │         gold_dim_provider
         │               │                     │
         └───────►gold_fact_encounters◄────────┤
                         │                     │
                         └────► gold_dim_payer │
                         │                     │
                         └──► gold_dim_encounter_type
```

### Componentes do Star Schema

**6 Dimensões:**
1. `gold_dim_date` - Calendário completo
2. `gold_dim_patient` - Informações de pacientes
3. `gold_dim_provider` - Médicos/hospitais
4. `gold_dim_payer` - Planos de saúde
5. `gold_dim_procedure` - Códigos de procedimentos
6. `gold_dim_encounter_type` - Tipos de atendimento

**3 Tabelas de Fatos:**
1. `gold_fact_claims` - Reivindicações (claims)
2. `gold_fact_encounters` - Atendimentos
3. `gold_fact_claim_transactions` - Transações detalhadas

---

## 🔍 Explicação do Código

### 1. Funções Auxiliares Essenciais

#### **A. `_get_unknown_value()` - Valores Unknown por Tipo**

```python
def _get_unknown_value(dtype):
    """Retorna valor 'Unknown' apropriado para cada tipo."""
    if pd.api.types.is_datetime64_any_dtype(dtype): 
        return pd.NaT
    if pd.api.types.is_numeric_dtype(dtype): 
        return pd.NA
    if pd.api.types.is_bool_dtype(dtype): 
        return False
    return 'Unknown'
```

**Por que isso é importante?**
- Cada tipo de dado tem seu "valor desconhecido" apropriado
- Evita erros de tipo ao criar registros Unknown

**Exemplos:**
```python
# String → 'Unknown'
# Integer → pd.NA (aceita nulos)
# Date → pd.NaT (Not a Time)
# Boolean → False
```

---

#### **B. `add_unknown_member()` - Adiciona Registro Unknown**

```python
def add_unknown_member(df, sk_col, natural_key_col, unknown_natural_key='UNKNOWN', 
                       unknown_sk=-1, **defaults):
    """
    Adiciona um membro 'Unknown' a uma dimensão.
    SK = -1 (convenção para dados faltantes).
    """
    unknown_row = {sk_col: unknown_sk, natural_key_col: unknown_natural_key}
    
    # Preenche outras colunas com valores padrão
    for col in df.columns:
        if col not in unknown_row:
            unknown_row[col] = defaults.get(col, _get_unknown_value(df[col].dtype))
    
    # Converte para DataFrame e ajusta tipos
    unknown_df = pd.DataFrame([unknown_row])
    
    # Garante compatibilidade de tipos antes de concatenar
    for col, dtype in df.dtypes.items():
        if col in unknown_df.columns:
            if pd.api.types.is_integer_dtype(dtype):
                unknown_df[col] = pd.to_numeric(unknown_df[col], errors='coerce').astype('Int64')
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                unknown_df[col] = pd.to_datetime(unknown_df[col], errors='coerce')
    
    # Adiciona Unknown como PRIMEIRA linha (SK = -1)
    return pd.concat([unknown_df, df], ignore_index=True)
```

**Resultado:**
```
Dimensão ANTES:
patient_sk | patient_natural_key | full_name
1          | P123                | João Silva
2          | P456                | Maria Santos

Dimensão DEPOIS (com Unknown):
patient_sk | patient_natural_key | full_name
-1         | UNKNOWN             | Unknown Patient  ← Adicionado
1          | P123                | João Silva
2          | P456                | Maria Santos
```

**Por que SK = -1 para Unknown?**
- ✅ Fácil identificar em queries (`WHERE patient_sk != -1`)
- ✅ Nunca conflita com SKs reais (sempre positivos)
- ✅ Padrão da indústria em Data Warehousing

---

#### **C. `add_audit_columns()` - Auditoria da Gold**

```python
def add_audit_columns(df):
    """Adiciona timestamps de criação/atualização na Gold."""
    now_utc = datetime.now(pytz.utc).replace(microsecond=0)
    return df.assign(
        dw_gold_created_at=now_utc, 
        dw_gold_updated_at=now_utc
    )
```

**Tracking completo:**
```
Bronze: dw_created_at, dw_updated_at
Silver: dw_created_at, dw_updated_at  
Gold:   dw_gold_created_at, dw_gold_updated_at ← Rastreia processamento final
```

---

#### **D. `merge_and_fill_sk()` - Adiciona SKs de Data**

```python
def merge_and_fill_sk(df, dim_df, on_col_key, sk_col_name, original_sk_col_to_int=None):
    """
    Faz JOIN com dim_date e adiciona SK de data.
    Preenche com -1 se data não encontrada.
    """
    # 1. Trata SKs existentes (converte para Int64)
    if original_sk_col_to_int:
        df[original_sk_col_to_int] = df[original_sk_col_to_int].fillna(-1).astype('Int64')
    
    # 2. JOIN com dimensão de data
    merged = pd.merge(df, dim_df[['date_key', 'date_sk']], 
                      left_on=on_col_key, right_on='date_key', how='left') \
               .rename(columns={'date_sk': sk_col_name}) \
               .drop(columns=['date_key'])
    
    # 3. Preenche SK de data com -1 se não encontrada
    merged[sk_col_name] = merged[sk_col_name].fillna(-1).astype('Int64')
    
    return merged
```

**Fluxo:**
```
Fato ANTES:
claim_id | claim_start_date
C001     | 2024-01-15

↓ merge_and_fill_sk()

Fato DEPOIS:
claim_id | claim_start_date | claim_start_date_sk
C001     | 2024-01-15       | 12345  ← SK da data
```

---

### 2. Construção das Dimensões

#### **A. Dimensão de Data (`dim_date`)**

```python
def create_dim_date(start_date, end_date):
    """Cria dimensão de data completa (calendário)."""
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    dim_date_df = pd.DataFrame({
        'date_sk': np.arange(1, len(dates) + 1),
        'date_key': dates.strftime('%Y-%m-%d'),  # '2024-01-15'
        'year': dates.year,                      # 2024
        'quarter': dates.quarter,                # 1
        'month': dates.month,                    # 1
        'day': dates.day,                        # 15
        'day_of_week': dates.dayofweek,          # 0 (segunda)
        'day_name': dates.day_name(),            # 'Monday'
        'month_name': dates.month_name(),        # 'January'
        'week_of_year': dates.isocalendar().week.astype(int),
        'is_weekend': (dates.dayofweek >= 5).astype(bool)  # True/False
    })
    
    return dim_date_df.pipe(add_unknown_member, 'date_sk', 'date_key', '9999-12-31',
                            year=9999, quarter=99, month=99, day=99, 
                            day_of_week=9, day_name='Unknown', 
                            month_name='Unknown', week_of_year=99, 
                            is_weekend=False) \
                      .pipe(add_audit_columns)
```

**Resultado:**
```
date_sk | date_key   | year | quarter | month | day | day_name | is_weekend
-1      | 9999-12-31 | 9999 | 99      | 99    | 99  | Unknown  | False      ← Unknown
1       | 2024-01-01 | 2024 | 1       | 1     | 1   | Monday   | False
2       | 2024-01-02 | 2024 | 1       | 1     | 2   | Tuesday  | False
...
```

**Benefícios:**
- ✅ Queries rápidas: `WHERE year = 2024 AND quarter = 1`
- ✅ Agregações fáceis: `GROUP BY month_name`
- ✅ Análises de sazonalidade: `is_weekend`, `day_of_week`

---

#### **B. Dimensão de Paciente (`dim_patient`)**

```python
def build_dim_patient(silver_patients_df):
    """Constrói dimensão de paciente a partir da Silver."""
    silver_patients_df['age'] = silver_patients_df['age'].astype('Int64')
    
    return silver_patients_df.rename(columns={'patient_id': 'patient_natural_key'}) \
                             .pipe(add_unknown_member, 'patient_sk', 'patient_natural_key',
                                   full_name='Unknown Patient', 
                                   date_of_birth=pd.NaT, 
                                   age=pd.NA, 
                                   age_group='Unknown') \
                             .pipe(add_audit_columns)
```

**Transformação aplicada:**
```
Silver (entrada):
patient_sk | patient_id | full_name   | age | age_group
1          | P123       | João Silva  | 39  | 36-45

Gold (saída):
patient_sk | patient_natural_key | full_name   | age | age_group | dw_gold_created_at
-1         | UNKNOWN             | Unknown     | NA  | Unknown   | 2024-11-11 10:00:00 ← Unknown
1          | P123                | João Silva  | 39  | 36-45     | 2024-11-11 10:00:00
```

**Mudanças Silver → Gold:**
1. `patient_id` → `patient_natural_key` (renomeado para clareza)
2. Unknown member adicionado (SK = -1)
3. Auditoria Gold adicionada

---

#### **C. Dimensão Genérica (`build_generic_dimension()`)**

```python
def build_generic_dimension(source_df, natural_key_col_source, sk_col_name, 
                            natural_key_col_name, description_col_name=None):
    """Constrói dimensão a partir de uma coluna de origem."""
    
    # 1. Extrai valores únicos
    dim_df = source_df[[natural_key_col_source]].drop_duplicates().dropna() \
                       .reset_index(drop=True) \
                       .rename(columns={natural_key_col_source: natural_key_col_name})
    
    # 2. Gera SK sequencial
    dim_df[sk_col_name] = np.arange(1, len(dim_df) + 1)
    
    # 3. Adiciona descrição (opcional)
    if description_col_name:
        dim_df[description_col_name] = dim_df[natural_key_col_name].apply(
            lambda x: f"{natural_key_col_name.replace('_', ' ').title()} {x}"
        )
    
    # 4. Adiciona Unknown e auditoria
    return dim_df.pipe(add_unknown_member, sk_col_name, natural_key_col_name,
                       **({description_col_name: f"Unknown {natural_key_col_name}"} 
                          if description_col_name else {})) \
                 .pipe(add_audit_columns)
```

---

### 3. Construção das Tabelas de Fatos

#### **A. Fato: Claims**

```python
def build_fact_claims(silver_claims_df, dim_date_df):
    """Constrói tabela de fatos de claims."""
    
    # 1. Garante que SKs existentes sejam Int64
    silver_claims_df['patient_sk'] = silver_claims_df['patient_sk'].fillna(-1).astype('Int64')
    silver_claims_df['provider_sk'] = silver_claims_df['provider_sk'].fillna(-1).astype('Int64')
    
    # 2. Cria chaves de data (strings formatadas)
    return silver_claims_df.assign(
        claim_start_date_key=lambda df: df['claim_start_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31'),
        claim_end_date_key=lambda df: df['claim_end_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31')
    # 3. Adiciona SKs de data (JOINs com dim_date)
    ).pipe(merge_and_fill_sk, dim_date_df, 'claim_start_date_key', 'claim_start_date_sk') \
     .pipe(merge_and_fill_sk, dim_date_df, 'claim_end_date_key', 'claim_end_date_sk') \
    # 4. Adiciona auditoria
     .pipe(add_audit_columns) \
    # 5. Seleciona colunas finais (apenas SKs e métricas)
     [['claim_id', 'patient_sk', 'provider_sk', 'claim_start_date_sk', 
       'claim_end_date_sk', 'total_outstanding', 'dw_gold_created_at', 
       'dw_gold_updated_at']]
```

**Transformação Silver → Gold:**

**Silver (entrada):**
```
claim_id | patient_sk | provider_sk | claim_start_date | total_outstanding
C001     | 1          | 5           | 2024-01-15       | 150.75
```

**Gold (saída):**
```
claim_id | patient_sk | provider_sk | claim_start_date_sk | total_outstanding
C001     | 1          | 5           | 12345               | 150.75
         └────────────┴─────────────────────────────────────────┴─ Apenas SKs numéricas!
```

**Vantagens:**
- ✅ Apenas números (performance)
- ✅ JOINs extremamente rápidos
- ✅ Compatível com qualquer ferramenta de BI

---

#### **B. Fato: Encounters**

```python
def build_fact_encounters(silver_encounters_df, dim_date_df, dim_encounter_type_df):
    """Constrói tabela de fatos de encontros."""
    
    # 1. Garante SKs Int64
    silver_encounters_df['patient_sk'] = silver_encounters_df['patient_sk'].fillna(-1).astype('Int64')
    silver_encounters_df['provider_sk'] = silver_encounters_df['provider_sk'].fillna(-1).astype('Int64')
    silver_encounters_df['payer_sk'] = silver_encounters_df['payer_sk'].fillna(-1).astype('Int64')
    
    # 2. Cria chaves de data
    return silver_encounters_df.assign(
        encounter_date_key=lambda df: df['encounter_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31'),
        discharge_date_key=lambda df: df['discharge_date'].dt.strftime('%Y-%m-%d').fillna('9999-12-31')
    # 3. Adiciona SKs de data
    ).pipe(merge_and_fill_sk, dim_date_df, 'encounter_date_key', 'encounter_date_sk') \
     .pipe(merge_and_fill_sk, dim_date_df, 'discharge_date_key', 'discharge_date_sk') \
    # 4. Adiciona SK de tipo de encontro (JOIN)
     .merge(dim_encounter_type_df[['encounter_type', 'encounter_type_sk']], 
            on='encounter_type', how='left') \
     .assign(encounter_type_sk=lambda df: df['encounter_type_sk'].fillna(-1).astype('Int64')) \
    # 5. Auditoria
     .pipe(add_audit_columns) \
    # 6. Seleciona colunas
     [['encounter_id', 'patient_sk', 'provider_sk', 'payer_sk', 'encounter_type_sk',
       'encounter_date_sk', 'discharge_date_sk', 'total_claim_cost', 
       'payer_coverage', 'length_of_stay_days', 'dw_gold_created_at', 
       'dw_gold_updated_at']]
```

**Métricas importantes:**
- `total_claim_cost`: Custo total do atendimento
- `payer_coverage`: Cobertura do plano de saúde
- `length_of_stay_days`: Tempo de internação (calculado na Silver)

---

### 4. Configurações de Schema (DIMENSION_CONFIGS / FACT_CONFIGS)

```python
DIMENSION_CONFIGS = {
    "gold_dim_date": {
        "builder": create_dim_date,  # Função que constrói a dimensão
        "params": lambda silver_data, min_date, max_date: [min_date, max_date],
        "dtypes": {  # Tipos SQLAlchemy para PostgreSQL
            'date_sk': types.BigInteger,
            'date_key': types.String(10),
            'year': types.SmallInteger,
            'is_weekend': types.Boolean,
            'dw_gold_created_at': types.TIMESTAMP(timezone=True),
            # ... outros campos
        }
    },
    "gold_dim_patient": {
        "builder": build_dim_patient,
        "params": lambda silver_data, *args: [silver_data["silver_dim_patient"]],
        "dtypes": {
            'patient_sk': types.BigInteger,
            'patient_natural_key': types.String(50),
            'full_name': types.String(255),
            'date_of_birth': types.Date,
            'age': types.SmallInteger,
            # ...
        }
    },
    # ... outras dimensões
}
```

**Por que usar configurações?**
- ✅ **DRY (Don't Repeat Yourself)**: Evita código duplicado
- ✅ **Manutenção fácil**: Alterar schema em um só lugar
- ✅ **Controle de tipos**: Garante tipos corretos no PostgreSQL
- ✅ **Escalabilidade**: Fácil adicionar novas dimensões/fatos

---

### 5. Função Principal: `load_gold()`

```python
def load_gold():
    engine = get_engine()
    if engine is None: return
    
    silver_data = {}
    gold_data = {}
    
    # 1. EXTRAI dados da Silver
    print("Reading Silver layer data...")
    silver_data["silver_dim_patient"] = pd.read_sql("SELECT * FROM silver_dim_patient", engine)
    silver_data["silver_dim_patient"]['date_of_birth'] = pd.to_datetime(
        silver_data["silver_dim_patient"]['date_of_birth'], errors='coerce'
    ).dt.date
    silver_data["silver_dim_patient"]['age'] = silver_data["silver_dim_patient"]['age'].astype('Int64')
    
    silver_data["silver_fact_claim"] = pd.read_sql(
        "SELECT * FROM silver_fact_claim", engine, 
        parse_dates=['claim_start_date', 'claim_end_date']
    )
    # ... outras tabelas Silver
    
    # 2. CALCULA range de datas para dim_date
    print("Calculating date range...")
    all_dates = pd.concat([
        silver_data["silver_fact_claim"]['claim_start_date'],
        silver_data["silver_fact_claim"]['claim_end_date'],
        silver_data["silver_fact_encounter"]['encounter_date'],
        # ...
    ]).dropna().drop_duplicates()
    
    min_date = all_dates.min()
    max_date = all_dates.max()
    
    # 3. CONSTRÓI Dimensões (ordem independente)
    print("Building Gold Dimensions...")
    for table_name, config in DIMENSION_CONFIGS.items():
        print(f"  Building {table_name}...")
        params = config["params"](silver_data, min_date, max_date)
        gold_data[table_name] = config["builder"](*params)
        gold_data[table_name].to_sql(table_name, engine, if_exists="replace", 
                                      index=False, dtype=config["dtypes"])
    
    # 4. CONSTRÓI Fatos (dependem das Dimensões)
    print("Building Gold Fact Tables...")
    for table_name, config in FACT_CONFIGS.items():
        print(f"  Building {table_name}...")
        params = config["params"](silver_data, gold_data, min_date, max_date)
        gold_data[table_name] = config["builder"](*params)
        gold_data[table_name].to_sql(table_name, engine, if_exists="replace", 
                                      index=False, dtype=config["dtypes"])
    
    print("\n✅ Gold layer loaded successfully (Star Schema built).")
```

**Ordem crítica de execução:**
```
1. Extrair Silver → DataFrames pandas
2. Calcular range de datas (min/max)
3. Construir TODAS as Dimensões → Banco
4. Construir TODOS os Fatos (usam SKs das Dimensões) → Banco
```

---

