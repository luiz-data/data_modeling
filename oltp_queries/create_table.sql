-- Tabela bronze_patients
CREATE TABLE bronze_patients (
    patient_id VARCHAR(255) PRIMARY KEY,
    date_of_birth DATE,
    first_name VARCHAR(255),
    last_name VARCHAR(255)
);

-- Tabela bronze_claims
CREATE TABLE bronze_claims (
    claim_id VARCHAR(255) PRIMARY KEY,
    patient_id VARCHAR(255),
    provider_id VARCHAR(255),
    claim_start_date DATE,
    claim_end_date DATE,
    outstanding_primary DECIMAL(10, 2),
    outstanding_secondary DECIMAL(10, 2),
    outstanding_patient DECIMAL(10, 2),
    FOREIGN KEY (patient_id) REFERENCES bronze_patients(patient_id)
);

-- Tabela bronze_claims_transactions
CREATE TABLE bronze_claims_transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    claim_id VARCHAR(255),
    patient_id VARCHAR(255),
    provider_id VARCHAR(255),
    transaction_date DATE,
    transaction_amount DECIMAL(10, 2),
    procedure_code VARCHAR(255),
    FOREIGN KEY (claim_id) REFERENCES bronze_claims(claim_id),
    FOREIGN KEY (patient_id) REFERENCES bronze_patients(patient_id)
);

-- Tabela bronze_payers
CREATE TABLE bronze_payers (
    payer_id VARCHAR(255) PRIMARY KEY,
    payer_name VARCHAR(255)
);

-- Tabela bronze_encounters
CREATE TABLE bronze_encounters (
    encounter_id VARCHAR(255) PRIMARY KEY,
    encounter_date DATE,
    discharge_date DATE,
    patient_id VARCHAR(255),
    provider_id VARCHAR(255),
    payer_id VARCHAR(255),
    encounter_type VARCHAR(255),
    total_claim_cost DECIMAL(10, 2),
    payer_coverage DECIMAL(10, 2),
    FOREIGN KEY (patient_id) REFERENCES bronze_patients(patient_id),
    FOREIGN KEY (payer_id) REFERENCES bronze_payers(payer_id)
);