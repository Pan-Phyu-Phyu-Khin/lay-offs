-- Data Cleaning

select * 
from layoffs;

-- 1. remove duplicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. removes any columns or rows


create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(
partition by company, industry, total_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off,
stage, country, funds_raised_millions, `date`) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select * 
from layoffs_staging
where company = 'Casper';

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off,
stage, country, funds_raised_millions, `date`) as row_num
from layoffs_staging
)
delete 
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging3;

insert into layoffs_staging3
select *,
row_number() over(
partition by company, location, industry, total_laid_off,
stage, country, funds_raised_millions, `date`) as row_num
from layoffs_staging
;

select * 
from layoffs_staging3
where row_num > 1;

delete 
from layoffs_staging3
where row_num > 1;

select * 
from layoffs_staging3
where row_num > 1;

select * 
from layoffs_staging3;


-- 2. standardize the data

select company, trim(company)
from layoffs_staging3;

update layoffs_staging3
set company = trim(company);

select *
from layoffs_staging3
where industry like 'crypto%';

update layoffs_staging3
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct(industry)
from layoffs_staging3;

select distinct(country)
from layoffs_staging3
order by 1;

select *
from layoffs_staging3
where country like '%United State%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging3
;

update layoffs_staging3
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`, 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging3;

update layoffs_staging3
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging3
modify column `date` date;

select * 
from layoffs_staging3;

select *
from layoffs_staging3
where industry is null
or industry = ''
;

select * 
from layoffs_staging3
where company like 'Bally%';

select * 
from layoffs_staging3
where company = 'Airbnb';

select *
from layoffs_staging3 as t1
join layoffs_staging3 as t3
	on t1.company = t3.company
	and t1.location = t3.location
where (t1.industry is null or t1.industry = '')
and t3.industry is not null;

select t1.industry, t3.industry
from layoffs_staging3 as t1
join layoffs_staging3 as t3
	on t1.company = t3.company
	and t1.location = t3.location
where (t1.industry is null or t1.industry = '')
and t3.industry is not null;

update layoffs_staging3
set industry = null
where industry = '';

update layoffs_staging3 as t1
join layoffs_staging3 as t3
	on t1.company = t3.company
set t1.industry = t3.industry
where t1.industry is null
and t3.industry is not null;

select * 
from layoffs_staging3
order by 1;

select * 
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

select * 
from layoffs_staging3;

select max(date), min(date) 
from layoffs_staging3
order by date
;

alter table layoffs_staging3
drop column row_num;



