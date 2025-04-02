-- Data Cleaning

SELECT *
FROM layoffs;

-- The first thing we did was create a staging table to work with the raw data. 
-- This is the one we will work in and clean the data.

CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- These are the steps we will follow when cleaning data.
-- 1. Check for Duplicates and Remove if Any
-- 2. Standardize and Fix Errors
-- 3. Review Null Values/Blanks
-- 4. Remove Any Unnecessary Columns

-- 1. Remove Duplicates
-- First we will check for duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Looked at 23andMe to confirm duplicates
SELECT *
FROM layoffs_staging
WHERE company = '23andMe';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num >1;

SELECT *
FROM layoffs_staging2;

-- Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Dapper Labs has Canada listed as country. All other companies with Vancounver/Canada are listed as Non US. This will be updated to match. 
SELECT *
FROM layoffs_staging2
WHERE location LIKE '%Vancouver%';

UPDATE layoffs_staging2
SET location = "['Vancouver'', ''Non-U.S.']"
WHERE location LIKE '%Vancouver%'
AND country = 'Canada';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- We also need to update the date column
SELECT `date`,
	DATE(str_to_date(`date`, '%Y-%m-%dT%H:%i:%s.000Z'))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = DATE(str_to_date(`date`, '%Y-%m-%dT%H:%i:%s.000Z'))

-- Change data type to date 

ALTER TABLE layoffs_staging2
MODIFY COLUMN `DATE` DATE; 

-- Looking at Null Values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = ''
;


SELECT *
FROM layoffs_staging2
WHERE industry = ''
;

-- There wasn't anything we could change with the blank values
-- We'll leave them as is for the EDA phase.

-- 4. Remove any unnecessary columns and rows.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = ''
;

-- There are no rows with blank values from both total_laid_off AND percentage_laid_off so we won't remove these.

-- Finally, we will get rid of column row_num since we no longer need it.

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;