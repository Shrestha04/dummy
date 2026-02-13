============================================================
PHASE 1: DATA INGESTION (RAW LAYER)
============================================================

-- Step 1: Create Database
CREATE DATABASE healthcare_dw;
USE healthcare_dw;

-- Step 2: Create Raw Table
CREATE TABLE raw_pima_data (
    pregnancies INT,
    glucose INT,
    blood_pressure INT,
    skin_thickness INT,
    insulin INT,
    bmi DECIMAL(5,2),
    diabetes_pedigree DECIMAL(6,4),
    age INT,
    outcome INT
);

-- Step 3: Load CSV
LOAD DATA INFILE 'pima.csv'
INTO TABLE raw_pima_data
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

============================================================
PHASE 2: ETL – DATA CLEANING & TRANSFORMATION
============================================================

-- Step 4: Create Staging Table
CREATE TABLE stage_pima_data AS
SELECT *
FROM raw_pima_data
WHERE glucose > 0
  AND blood_pressure > 0
  AND bmi > 0;

-- Step 5: Add Derived BMI Category
ALTER TABLE stage_pima_data ADD COLUMN bmi_category VARCHAR(20);

UPDATE stage_pima_data
SET bmi_category =
    CASE
        WHEN bmi < 18.5 THEN 'Underweight'
        WHEN bmi BETWEEN 18.5 AND 24.9 THEN 'Normal'
        WHEN bmi BETWEEN 25 AND 29.9 THEN 'Overweight'
        ELSE 'Obese'
    END;

============================================================
PHASE 3: STAR SCHEMA DESIGN
============================================================

-- DIMENSION TABLES
CREATE TABLE dim_patient (
    patient_key INT PRIMARY KEY AUTO_INCREMENT,
    pregnancies INT,
    diabetes_pedigree DECIMAL(6,4)
);

CREATE TABLE dim_bmi (
    bmi_key INT PRIMARY KEY AUTO_INCREMENT,
    bmi_category VARCHAR(20)
);

CREATE TABLE dim_age_group (
    age_key INT PRIMARY KEY AUTO_INCREMENT,
    age_group VARCHAR(20)
);

CREATE TABLE dim_glucose (
    glucose_key INT PRIMARY KEY AUTO_INCREMENT,
    glucose_level VARCHAR(20)
);

-- FACT TABLE
CREATE TABLE fact_diabetes (
    fact_key INT PRIMARY KEY AUTO_INCREMENT,
    patient_key INT,
    bmi_key INT,
    age_key INT,
    glucose_key INT,
    outcome INT,
    bmi DECIMAL(5,2),
    glucose INT,
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (bmi_key) REFERENCES dim_bmi(bmi_key),
    FOREIGN KEY (age_key) REFERENCES dim_age_group(age_key),
    FOREIGN KEY (glucose_key) REFERENCES dim_glucose(glucose_key)
);

============================================================
PHASE 4: INSERT DATA INTO DIMENSION TABLES
============================================================

-- STEP 1: INSERT INTO dim_patient
INSERT INTO dim_patient (pregnancies, diabetes_pedigree)
SELECT DISTINCT pregnancies, diabetes_pedigree
FROM stage_pima_data;

-- STEP 2: INSERT INTO dim_bmi
INSERT INTO dim_bmi (bmi_category)
SELECT DISTINCT bmi_category
FROM stage_pima_data;

-- STEP 3: CREATE & INSERT INTO dim_age_group
ALTER TABLE stage_pima_data ADD COLUMN age_group VARCHAR(20);

UPDATE stage_pima_data
SET age_group =
    CASE
        WHEN age BETWEEN 20 AND 30 THEN '20-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        ELSE '51+'
    END;

INSERT INTO dim_age_group (age_group)
SELECT DISTINCT age_group
FROM stage_pima_data;

-- STEP 4: CREATE & INSERT INTO dim_glucose
ALTER TABLE stage_pima_data ADD COLUMN glucose_level VARCHAR(20);

UPDATE stage_pima_data
SET glucose_level =
    CASE
        WHEN glucose < 100 THEN 'Normal'
        WHEN glucose BETWEEN 100 AND 125 THEN 'Prediabetic'
        ELSE 'Diabetic'
    END;

INSERT INTO dim_glucose (glucose_level)
SELECT DISTINCT glucose_level
FROM stage_pima_data;

============================================================
STEP 5: INSERT INTO FACT TABLE (Surrogate Key Lookup)
============================================================

INSERT INTO fact_diabetes (
    patient_key, bmi_key, age_key, glucose_key,
    outcome, bmi, glucose
)
SELECT
    p.patient_key,
    b.bmi_key,
    a.age_key,
    g.glucose_key,
    s.outcome,
    s.bmi,
    s.glucose
FROM stage_pima_data s
JOIN dim_patient p
    ON s.pregnancies = p.pregnancies
   AND s.diabetes_pedigree = p.diabetes_pedigree
JOIN dim_bmi b
    ON s.bmi_category = b.bmi_category
JOIN dim_age_group a
    ON s.age_group = a.age_group
JOIN dim_glucose g
    ON s.glucose_level = g.glucose_level;

============================================================
PHASE 6: ANALYTICAL QUERIES
============================================================

-- 1. Diabetes rate by Age Group
SELECT 
    a.age_group,
    COUNT(*) AS total_patients,
    SUM(outcome) AS diabetic_cases
FROM fact_diabetes f
JOIN dim_age_group a ON f.age_key = a.age_key
GROUP BY a.age_group;

-- 2. Diabetes by BMI Category
SELECT 
    b.bmi_category,
    SUM(outcome) AS diabetic_count
FROM fact_diabetes f
JOIN dim_bmi b ON f.bmi_key = b.bmi_key
GROUP BY b.bmi_category;

-- 3. Average Glucose by Outcome
SELECT 
    outcome,
    AVG(glucose) AS avg_glucose
FROM fact_diabetes
GROUP BY outcome;

============================================================
END OF PROJECT DOCUMENT
============================================================
