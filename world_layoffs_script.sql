-- Data Cleaning 

Select *
From layoffs;

-- Create Staging 
-- Remove Duplicates
-- Standardize Data
-- Review Null or Blank Values
-- Remove any columns or rows if needed


-- Created Staging table to keep integrity of orginal Dataset 
Create Table layoffs_staging
like layoffs;


Select *
From layoffs_staging;
-- Created the Columns now will insert data; essentially  making a copy of the original data 
Insert layoffs_staging 
select *
from layoffs;

-- noticing this dataset does not have an identifying column will create one
-- will help to identify any duplicates 

SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
-- used backtick because Date is a keyword
-- will use new column to check for duplicate
-- filtering using a cte

WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- DOUBLE CHECKING DUPLICATES
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- with mysql unable to delete directly from cte 
-- will need to create a staging 2 db and delete from there


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- created table with new identifier column
-- insert db 
SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- filter out duplicates from new table with new column
SELECT *
FROM layoffs_staging2
WHERE  row_num > 1;

-- after identifying the duplicate rows, delete 
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Double check 
SELECT *
FROM layoffs_staging2
WHERE  row_num > 1;

-- STANDARDIZING DATA
-- deleting white space in front or behind text 

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardizing column by column as needed
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- change name of idustry to match 

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
-- Updated 3 rows 
-- Double check for any other updates needed

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT DISTINCT location
FROM layoffs_staging2
order by 1;
 -- Looking through columns for any data that needs to be changed 
 SELECT DISTINCT country
 FROM layoffs_staging2
 ORDER BY 1;
 
 -- Found same country twice; one with period and one without
 SELECT DISTINCT country
 FROM layoffs_staging2
 WHERE country LIKE 'United States%';
 
 -- Use trailing function from trim to remove period
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Double check to see if updates were made 
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
 
 -- Our date column has a text data type so will change format
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- took the text data in the date column and changed to datetime format
-- Update column with new format keeping text data type 
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');
 
 -- Change text data type to datetime using altering
 ALTER TABLE layoffs_staging2
 MODIFY COLUMN `date` DATE;
 
 SELECT *
 FROM layoffs_staging2;
 
 -- REVIEWING NULL AND BLANK VALUES
 SELECT *
 FROM layoffs_staging2
 WHERE industry IS NULL
 OR industry = '';
 
 -- Check to see if another entry with same company has the missing data filled
 SELECT *
 FROM layoffs_staging2
 WHERE company = 'Airbnb';
 
 -- Populate data in industry column from existing rows 
 -- Will need to utilize join to self to populate
 SELECT *
 FROM layoffs_staging2 t1
 JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; 

-- Now will update using t2 to populate t1 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
 -- did not work; zero rows were affected
 
 -- will see if converting blanks to null values then trying again will work
 UPDATE layoffs_staging2
 SET industry = null
 WHERE industry = '';
 
 -- updated blank values to null values and will populate null values using exiting info
 UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
-- confirmed with action output

 -- Checking other columns 
 SELECT *
 FROM layoffs_staging2
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 -- Having both columns null renders the entries virtually useless for reviewing data regarding layoffs
 DELETE 
 FROM layoffs_staging2
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 SELECT *
 FROM layoffs_staging2;
 
 -- delete the identifier column we created in the beginning
 ALTER TABLE layoffs_staging2
 DROP COLUMN row_num;

