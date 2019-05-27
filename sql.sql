select
	sum(case when sat_name='好评' then 1 else 0 end)/count(a.id)
from a left join b
 on a.goods_id=b.good_id
where user_name='小明'
and   brand_name='花王'
and   bu_name='母婴'
and   substr(sub_time,1,10) between '2018-01-01' and '2018-01-31'
group by user_name,bu_name,user_name

select b.brand_name,sum(a.sales_amt)

from a,b
where a.SKU_ID=b.SKU_ID
 and  b.user_name='小明'
 and  a.logday like '2017%'
 group by b.brand_name
 order by sum(a.sales_amt) desc
 limit 0,3
 
 
 
 select * 
 from emp as a 
 where 3> (select count(*) from emp where deptno = a.deptno and sal < a.sal  )  
 order by a.deptno ,a.sal
 
 
select a.logday,b.brand_name,sum(a.sales_amt)

from a,b
where a.SKU_ID=b.SKU_ID
 and  b.user_name='小明'
 and  a.logday like '2017%'
 group by b.brand_name
 order sum(a.sales_amt)
 
 
 select a.* 
 from tb a 
 where 2 > (select count(*) from tb where name = a.name and val > a.val ) 
 order by a.name,a.val



