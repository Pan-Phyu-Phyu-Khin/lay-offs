-- Exploratory Data Analysis

select *
from layoffs_staging3;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging3;

select *
from layoffs_staging3
where percentage_laid_off = 1
order by funds_raised_millions desc;

select country, sum(total_laid_off)
from layoffs_staging3
group by country
order by 2 desc;

select min(date), max(date)
from layoffs_staging3;

select year(`date`), sum(total_laid_off)
from layoffs_staging3
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging3
group by stage
order by 2 desc;

select substr(`date`, 1,7) as months, sum(total_laid_off)
from layoffs_staging3
where substr(`date`, 1,7) is not null
group by months
order by 1 asc;

with Total_Rolling as
(
select substr(`date`, 1,7) as months, sum(total_laid_off) as total_off
from layoffs_staging3
where substr(`date`, 1,7) is not null
group by months
order by 1 asc
)
select months, total_off, 
sum(total_off) over(order by months) as total_rolling
from Total_Rolling;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
order by 3 desc;

with Company_Year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
)
select *, 
dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from Company_Year
where years is not null
order by Ranking asc;

with Company_Year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
), Company_Year_Rank as
(
select *, 
dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from Company_Year
where years is not null
)
select *
from Company_Year_Rank
where Ranking <= 5
;
select max(date), min(date)
from layoffs_staging3
order by date;



