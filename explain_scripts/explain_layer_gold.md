# 🥉 Camada Bronze - Guia de Implementação

## 📋 Índice
- [O que é a Camada Bronze?](#o-que-é-a-camada-bronze)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Explicação do Código](#explicação-do-código)
- [Como Executar](#como-executar)
- [Boas Práticas](#boas-práticas)

---

## 🎯 O que é a Camada Bronze?

A **Camada Bronze** é a primeira camada da Arquitetura Medallion. Seu objetivo é armazenar dados **brutos e sem transformações** vindos dos sistemas fonte (OLTP).

### Princípios
- ✅ **Dados brutos**: Sem limpeza ou transformação
- ✅ **Histórico completo**: Mantém todas as ingestões
- ✅ **Auditável**: Rastreabilidade total dos dados
- ✅ **Alta performance**: Otimizada para escrita

---

## 📁 Estrutura do Projeto

```
project/
├── .env                    # Credenciais (NÃO COMMITAR!)
├── .env.example            # Template de configuração
├── scripts/
│   └── load_bronze.py      # Script de carga
└── oltp_queries/
    ├── create_table.sql    # DDL (estrutura das tabelas)
    └── insert_into.sql     # DML (inserção de dados)
```

---

## 🔍 Explicação do Código

### 1. Imports e Configuração

```python
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Carrega variáveis de ambiente do arquivo .env
path_to_env = "../.env"
load_dotenv(dotenv_path=path_to_env, override=True)
```

**Por quê?**
- `dotenv`: Gerencia credenciais de forma segura (fora do código)
- `sqlalchemy`: Abstração de banco de dados (fácil trocar PostgreSQL por MySQL)
- `path_to_env`: Caminho relativo ao arquivo `.env`

---

### 2. Função `get_engine()` - Conexão com o Banco

```python
def get_engine(echo=False):
    # 1. Carrega credenciais do .env
    pg_user = os.getenv('PG_USER')
    pg_pass = os.getenv('PG_PASS')
    pg_host = os.getenv('PG_HOST')
    pg_port = os.getenv('PG_PORT')
    pg_db = os.getenv('PG_DB')
    
    # 2. Valida se todas as variáveis existem
    if not all([pg_user, pg_pass, pg_host, pg_port, pg_db]):
        raise ValueError("Variáveis de ambiente faltando no .env")
    
    # 3. Monta a URL de conexão
    url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    
    # 4. Cria o engine com pool de conexões
    engine = create_engine(url, pool_pre_ping=True, echo=echo)
    
    # 5. Testa a conexão
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    
    return engine
```

**Parâmetros importantes:**
- `pool_pre_ping=True`: Verifica se a conexão está viva antes de usar
- `echo=False`: Não exibe SQL no console (use `True` para debug)

**Tratamento de erros:**
```python
except ValueError as ve:
    print(f"❌ Erro de configuração: {ve}")
    return None
except SQLAlchemyError as sa_e:
    print(f"❌ Erro de conexão: {sa_e}")
    return None
```

---

### 3. Função `load_bronze()` - Carregamento dos Dados

```python
def load_bronze():
    # 1. Cria conexão
    engine = get_engine(echo=False)
    if engine is None:
        print("❌ Não foi possível conectar. Abortando.")
        return
    
    queries_path = "../oltp_queries"
    
    try:
        with engine.connect() as conn:
            # 2. Cria tabelas (DDL)
            create_sql = open(f"{queries_path}/create_table.sql", "r", encoding="utf-8").read()
            conn.execute(text(create_sql))
            conn.commit()
            print("✅ Tabelas criadas")
            
            # 3. Insere dados (DML)
            insert_sql = open(f"{queries_path}/insert_into.sql", "r", encoding="utf-8").read()
            conn.execute(text(insert_sql))
            conn.commit()
            print("✅ Dados inseridos")
    
    except FileNotFoundError as e:
        print(f"❌ Arquivo SQL não encontrado: {e}")
    except SQLAlchemyError as e:
        print(f"❌ Erro ao executar SQL: {e}")
```

**Fluxo de execução:**
```
1. get_engine()           → Cria conexão
2. create_table.sql       → DROP/CREATE tabelas
3. commit()               → Salva estrutura
4. insert_into.sql        → INSERT dados brutos
5. commit()               → Salva dados
```

---

## 🚀 Como Executar

### 1. Configure o arquivo `.env`

Crie um arquivo `.env` na raiz do projeto:

```env
# Configurações PostgreSQL
PG_USER=seu_usuario
PG_PASS=sua_senha
PG_HOST=localhost
PG_PORT=5432
PG_DB=bronze_db
```

⚠️ **IMPORTANTE:** Adicione `.env` ao `.gitignore`!

```bash
# .gitignore
.env
*.env
.venv/
__pycache__/
```

### 2. Instale as dependências

```bash
pip install python-dotenv sqlalchemy psycopg2-binary
```

### 3. Execute o script

```bash
python scripts/load_bronze.py
```

**Saída esperada:**
```
✅ Conexão estabelecida com sucesso.
✅ Tabelas criadas
✅ Dados inseridos
✅ Carga da camada Bronze concluída
```

---

## 🔒 Boas Práticas

### Segurança

**❌ Nunca faça isso:**
```python
pg_user = "admin"  # Credenciais no código
pg_pass = "123456"
```

**✅ Sempre faça assim:**
```python
pg_user = os.getenv('PG_USER')  # Credenciais no .env
```

### Mascaramento de Senhas em Logs

```python
def mask_password(password, visible=3):
    """Mascara senha para logs: 'senha123' → 'sen***'"""
    if not password or len(password) <= visible:
        return "***"
    return f"{password[:visible]}{'*' * (len(password) - visible)}"

# Uso
print(f"Conectando com senha: {mask_password(pg_pass)}")
# Output: Conectando com senha: sen***
```

### Template `.env.example`

Crie um arquivo `.env.example` (para o repositório):

```env
# Copie para .env e preencha com suas credenciais

PG_USER=seu_usuario_aqui
PG_PASS=sua_senha_aqui
PG_HOST=localhost
PG_PORT=5432
PG_DB=nome_do_banco
```

### Context Manager

```python
# ✅ Recomendado: with fecha a conexão automaticamente
with engine.connect() as conn:
    conn.execute(text("SELECT 1"))

# ❌ Evite: Você pode esquecer de fechar
conn = engine.connect()
conn.execute(text("SELECT 1"))
conn.close()  # Fácil esquecer!
```

---

## 📊 Arquitetura Medallion

```
OLTP (Sistema Fonte)
       ↓
🥉 BRONZE (Raw)          ← Você está aqui
   └─ Dados brutos
   └─ Sem transformações
       ↓
🥈 SILVER (Cleaned)
   └─ Dados limpos
   └─ Validações
       ↓
🥇 GOLD (Aggregated)
   └─ Star Schema
   └─ Pronto para BI
```

---


## 🔗 Referências

- [Arquitetura Medallion - Databricks](https://www.databricks.com/glossary/medallion-architecture)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Python dotenv](https://pypi.org/project/python-dotenv/)

---


