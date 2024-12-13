-- count number of athletes from each country
select country, count(*) as TotalAthletes
from athletes
group by Country
order by 2 desc;

-- calculate total no.of medals won by each country
select TeamCountry,
sum(Gold) as total_gold
, sum(Silver) as total_silver
, sum(Bronze) as total_bronze
from medals
group by TeamCountry
order by total_gold desc;

-- calculate avg no of entries by each gender for each discipline
select discipline,
avg(Female) as avg_female,
avg(Male) as avg_male
from entriesgender
group by discipline;

