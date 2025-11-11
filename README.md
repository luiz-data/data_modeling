
# 📊 Data Modeling - Star Schema ETL com Arquitetura Medallion



## 📌 Sobre o Projeto

Este projeto demonstra a implementação de uma **modelagem dimensional Star Schema** integrada a uma **arquitetura Medallion** (Bronze, Silver e Gold) para processos de ETL (Extract, Transform, Load). O objetivo é apresentar boas práticas de engenharia de dados, desde a ingestão de dados brutos até a criação de modelos analíticos otimizados para Business Intelligence.

> **⚠️ Nota Importante**: Todos os dados utilizados neste projeto são **fictícios**, gerados para fins educacionais e de demonstração. Embora sejam baseados em cenários e situações reais do mercado, não representam informações de nenhuma empresa ou entidade real.

## 🎯 Objetivos

- Demonstrar a aplicação prática da arquitetura Medallion em pipelines de dados
- Implementar uma modelagem dimensional Star Schema para análises otimizadas
- Apresentar técnicas de transformação e limpeza de dados
- Criar um modelo de dados escalável e de fácil manutenção
- Servir como material de referência para profissionais de dados

## 🏗️ Arquitetura

### Arquitetura Medallion

O projeto segue a arquitetura Medallion, organizada em três camadas:

```
Bronze (Raw Data)
    ↓
Silver (Cleaned & Validated)
    ↓
Gold (Business-Level Aggregates)
```

#### 🥉 Camada Bronze
- **Propósito**: Armazenamento de dados brutos sem transformações
- **Características**: 
  - Dados em formato original
  - Preservação histórica completa
  - Schema-on-read

#### 🥈 Camada Silver
- **Propósito**: Dados limpos, validados e padronizados
- **Características**:
  - Remoção de duplicatas
  - Tratamento de valores nulos
  - Padronização de formatos
  - Validação de qualidade de dados

#### 🥇 Camada Gold
- **Propósito**: Dados agregados prontos para consumo analítico
- **Características**:
  - Modelagem dimensional (Star Schema)
  - Dados otimizados para consultas
  - Métricas de negócio pré-calculadas

### Star Schema

- Consultas SQL simplificadas
- Performance otimizada para análises
- Fácil compreensão por usuários de negócio
- Escalabilidade para grandes volumes de dados

