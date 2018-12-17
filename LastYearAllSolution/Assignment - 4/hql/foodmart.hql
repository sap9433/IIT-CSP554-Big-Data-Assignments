use foodmart;
select p.product_name, i.units_shipped from product p join inventory_fact_1998 i on p.product_id = i.product_id;

