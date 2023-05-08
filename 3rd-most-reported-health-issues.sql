--Code run using MS SQL Server
select newfinal.facility_name
from (
select TOP 3 final.facility_name,final.qty
from (
select m.facility_name,count(m.program_element_pe) as qty
from los_angeles_restaurant_health_inspections as m
where m.facility_name like '%CAFE%' OR m.facility_name like '%TEA%' OR m.facility_name like '%JUICE%'
group by  m.facility_name
) as final
order by final.qty desc
) as newfinal
where newfinal.qty = (select MIN(qq.qty) from ( select top 3 final.qty
from (
select m.facility_name,count(m.program_element_pe) as qty
from los_angeles_restaurant_health_inspections as m
where m.facility_name like '%CAFE%' OR m.facility_name like '%TEA%' OR m.facility_name like '%JUICE%'
group by  m.facility_name
) as final
order by final.qty desc
) as qq
)