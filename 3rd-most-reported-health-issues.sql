select final.facility_name
from (
select q.facility_name, min(q.new_qty)
from (

select f.facility_name, max(f.qty) as new_qty
from (
select m.serial_number,m.facility_name,m.pe_description, count(m.program_element_pe) as qty
from los_angeles_restaurant_health_inspections as m
where m.facility_name like '%CAFE%'
group by  m.serial_number,m.facility_name,m.pe_description
) as f
group by f.facility_name

union

select w.facility_name, max(w.qty) as new_qty
from (
select m.serial_number,m.facility_name,m.pe_description, count(m.program_element_pe) as qty
from los_angeles_restaurant_health_inspections as m
where m.facility_name like '%TEA%'
group by  m.serial_number,m.facility_name,m.pe_description
) as w
group by w.facility_name

union

select r.facility_name, max(r.qty) as new_qty
from (

select m.serial_number,m.facility_name,m.pe_description, count(m.program_element_pe) as qty
from los_angeles_restaurant_health_inspections as m
where m.facility_name like '%JUICE%'
group by  m.serial_number,m.facility_name,m.pe_description
) as r
group by r.facility_name
) as q
group by q.facility_name
) as final