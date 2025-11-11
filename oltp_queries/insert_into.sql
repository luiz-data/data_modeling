-- Inserções para bronze_patients (20 registros)
INSERT INTO bronze_patients (patient_id, date_of_birth, first_name, last_name) VALUES
('PAT001', '1980-01-15', 'Alice', 'Silva'),
('PAT002', '1992-05-23', 'Bruno', 'Santos'),
('PAT003', '1975-11-01', 'Carla', 'Oliveira'),
('PAT004', '2001-03-10', 'Daniel', 'Pereira'),
('PAT005', '1968-09-20', 'Eva', 'Souza'),
('PAT006', '1985-02-28', 'Felipe', 'Costa'),
('PAT007', '1990-07-12', 'Gabriela', 'Martins'),
('PAT008', '1972-04-05', 'Heitor', 'Lima'),
('PAT009', '1998-12-18', 'Isabela', 'Fernandes'),
('PAT010', '1983-06-30', 'João', 'Gomes'),
('PAT011', '1970-08-08', 'Karen', 'Dias'),
('PAT012', '1995-01-25', 'Lucas', 'Rodrigues'),
('PAT013', '1988-10-14', 'Mariana', 'Almeida'),
('PAT014', '1965-03-03', 'Nuno', 'Ribeiro'),
('PAT015', '1993-09-07', 'Olívia', 'Carvalho'),
('PAT016', '1981-04-19', 'Pedro', 'Teixeira'),
('PAT017', '1999-02-22', 'Quintina', 'Nunes'),
('PAT018', '1978-07-01', 'Rafael', 'Pires'),
('PAT019', '1991-11-11', 'Sofia', 'Melo'),
('PAT020', '1973-05-09', 'Thiago', 'Coelho');

-- Inserções para bronze_payers (5 registros - para ter variedade, não 20 idênticos)
-- Vamos criar menos pagadores para que os claims e encounters possam se referir a um pool menor.
INSERT INTO bronze_payers (payer_id, payer_name) VALUES
('PAY001', 'Seguradora A'),
('PAY002', 'Plano de Saúde B'),
('PAY003', 'Seguradora C'),
('PAY004', 'Plano de Saúde D'),
('PAY005', 'Governo Saúde');

-- Inserções para bronze_claims (20 registros)
INSERT INTO bronze_claims (claim_id, patient_id, provider_id, claim_start_date, claim_end_date, outstanding_primary, outstanding_secondary, outstanding_patient) VALUES
('CLM001', 'PAT001', 'PROV001', '2023-01-01', '2023-01-05', 150.00, 50.00, 25.00),
('CLM002', 'PAT002', 'PROV002', '2023-01-10', '2023-01-12', 200.00, 75.00, 30.00),
('CLM003', 'PAT003', 'PROV003', '2023-01-15', '2023-01-15', 50.00, 0.00, 10.00),
('CLM004', 'PAT004', 'PROV001', '2023-01-20', '2023-01-22', 300.00, 100.00, 50.00),
('CLM005', 'PAT005', 'PROV004', '2023-02-01', '2023-02-07', 1200.00, 400.00, 200.00),
('CLM006', 'PAT006', 'PROV002', '2023-02-10', '2023-02-11', 80.00, 20.00, 15.00),
('CLM007', 'PAT007', 'PROV005', '2023-02-15', '2023-02-18', 600.00, 200.00, 100.00),
('CLM008', 'PAT008', 'PROV003', '2023-02-20', '2023-02-20', 75.00, 0.00, 12.00),
('CLM009', 'PAT009', 'PROV001', '2023-03-01', '2023-03-03', 180.00, 60.00, 28.00),
('CLM010', 'PAT010', 'PROV004', '2023-03-05', '2023-03-09', 900.00, 300.00, 150.00),
('CLM011', 'PAT011', 'PROV002', '2023-03-10', '2023-03-10', 45.00, 0.00, 8.00),
('CLM012', 'PAT012', 'PROV005', '2023-03-15', '2023-03-17', 250.00, 80.00, 40.00),
('CLM013', 'PAT013', 'PROV003', '2023-03-20', '2023-03-21', 110.00, 30.00, 20.00),
('CLM014', 'PAT014', 'PROV001', '2023-04-01', '2023-04-04', 700.00, 250.00, 120.00),
('CLM015', 'PAT015', 'PROV004', '2023-04-05', '2023-04-05', 60.00, 0.00, 10.00),
('CLM016', 'PAT016', 'PROV002', '2023-04-10', '2023-04-12', 320.00, 100.00, 55.00),
('CLM017', 'PAT017', 'PROV005', '2023-04-15', '2023-04-16', 130.00, 40.00, 22.00),
('CLM018', 'PAT018', 'PROV003', '2023-04-20', '2023-04-23', 800.00, 280.00, 140.00),
('CLM019', 'PAT019', 'PROV001', '2023-05-01', '2023-05-01', 90.00, 0.00, 18.00),
('CLM020', 'PAT020', 'PROV004', '2023-05-05', '2023-05-08', 400.00, 150.00, 70.00);

-- Inserções para bronze_claims_transactions (20 registros)
INSERT INTO bronze_claims_transactions (transaction_id, claim_id, patient_id, provider_id, transaction_date, transaction_amount, procedure_code) VALUES
('TRN001', 'CLM001', 'PAT001', 'PROV001', '2023-01-02', 75.00, 'PROC001'),
('TRN002', 'CLM001', 'PAT001', 'PROV001', '2023-01-03', 75.00, 'PROC002'),
('TRN003', 'CLM002', 'PAT002', 'PROV002', '2023-01-11', 100.00, 'PROC003'),
('TRN004', 'CLM002', 'PAT002', 'PROV002', '2023-01-12', 100.00, 'PROC004'),
('TRN005', 'CLM003', 'PAT003', 'PROV003', '2023-01-15', 50.00, 'PROC001'),
('TRN006', 'CLM004', 'PAT004', 'PROV001', '2023-01-21', 150.00, 'PROC005'),
('TRN007', 'CLM004', 'PAT004', 'PROV001', '2023-01-22', 150.00, 'PROC006'),
('TRN008', 'CLM005', 'PAT005', 'PROV004', '2023-02-03', 600.00, 'PROC007'),
('TRN009', 'CLM005', 'PAT005', 'PROV004', '2023-02-05', 600.00, 'PROC008'),
('TRN010', 'CLM006', 'PAT006', 'PROV002', '2023-02-10', 80.00, 'PROC001'),
('TRN011', 'CLM007', 'PAT007', 'PROV005', '2023-02-16', 300.00, 'PROC009'),
('TRN012', 'CLM007', 'PAT007', 'PROV005', '2023-02-17', 300.00, 'PROC010'),
('TRN013', 'CLM008', 'PAT008', 'PROV003', '2023-02-20', 75.00, 'PROC002'),
('TRN014', 'CLM009', 'PAT009', 'PROV001', '2023-03-02', 90.00, 'PROC003'),
('TRN015', 'CLM009', 'PAT009', 'PROV001', '2023-03-03', 90.00, 'PROC004'),
('TRN016', 'CLM010', 'PAT010', 'PROV004', '2023-03-07', 450.00, 'PROC005'),
('TRN017', 'CLM010', 'PAT010', 'PROV004', '2023-03-08', 450.00, 'PROC006'),
('TRN018', 'CLM011', 'PAT011', 'PROV002', '2023-03-10', 45.00, 'PROC001'),
('TRN019', 'CLM012', 'PAT012', 'PROV005', '2023-03-16', 125.00, 'PROC007'),
('TRN020', 'CLM012', 'PAT012', 'PROV005', '2023-03-17', 125.00, 'PROC008');

-- Inserções para bronze_encounters (20 registros)
INSERT INTO bronze_encounters (encounter_id, encounter_date, discharge_date, patient_id, provider_id, payer_id, encounter_type, total_claim_cost, payer_coverage) VALUES
('ENC001', '2023-01-01', '2023-01-05', 'PAT001', 'PROV001', 'PAY001', 'Hospitalization', 200.00, 150.00),
('ENC002', '2023-01-10', '2023-01-12', 'PAT002', 'PROV002', 'PAY002', 'Outpatient', 275.00, 200.00),
('ENC003', '2023-01-15', '2023-01-15', 'PAT003', 'PROV003', 'PAY001', 'Office Visit', 60.00, 50.00),
('ENC004', '2023-01-20', '2023-01-22', 'PAT004', 'PROV001', 'PAY003', 'Emergency', 450.00, 300.00),
('ENC005', '2023-02-01', '2023-02-07', 'PAT005', 'PROV004', 'PAY004', 'Hospitalization', 1800.00, 1200.00),
('ENC006', '2023-02-10', '2023-02-11', 'PAT006', 'PROV002', 'PAY002', 'Office Visit', 115.00, 80.00),
('ENC007', '2023-02-15', '2023-02-18', 'PAT007', 'PROV005', 'PAY005', 'Hospitalization', 900.00, 600.00),
('ENC008', '2023-02-20', '2023-02-20', 'PAT008', 'PROV003', 'PAY001', 'Office Visit', 87.00, 75.00),
('ENC009', '2023-03-01', '2023-03-03', 'PAT009', 'PROV001', 'PAY003', 'Outpatient', 268.00, 180.00),
('ENC010', '2023-03-05', '2023-03-09', 'PAT010', 'PROV004', 'PAY004', 'Hospitalization', 1350.00, 900.00),
('ENC011', '2023-03-10', '2023-03-10', 'PAT011', 'PROV002', 'PAY002', 'Office Visit', 53.00, 45.00),
('ENC012', '2023-03-15', '2023-03-17', 'PAT012', 'PROV005', 'PAY005', 'Emergency', 370.00, 250.00),
('ENC013', '2023-03-20', '2023-03-21', 'PAT013', 'PROV003', 'PAY001', 'Office Visit', 160.00, 110.00),
('ENC014', '2023-04-01', '2023-04-04', 'PAT014', 'PROV001', 'PAY003', 'Hospitalization', 1070.00, 700.00),
('ENC015', '2023-04-05', '2023-04-05', 'PAT015', 'PROV004', 'PAY004', 'Office Visit', 70.00, 60.00),
('ENC016', '2023-04-10', '2023-04-12', 'PAT016', 'PROV002', 'PAY002', 'Outpatient', 475.00, 320.00),
('ENC017', '2023-04-15', '2023-04-16', 'PAT017', 'PROV005', 'PAY005', 'Emergency', 192.00, 130.00),
('ENC018', '2023-04-20', '2023-04-23', 'PAT018', 'PROV003', 'PAY001', 'Hospitalization', 1220.00, 800.00),
('ENC019', '2023-05-01', '2023-05-01', 'PAT019', 'PROV001', 'PAY003', 'Office Visit', 108.00, 90.00),
('ENC020', '2023-05-05', '2023-05-08', 'PAT020', 'PROV004', 'PAY004', 'Outpatient', 620.00, 400.00);