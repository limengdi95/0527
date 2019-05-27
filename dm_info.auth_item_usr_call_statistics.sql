-------------------------------------------------------------------
-- Purpose：安全授权项用户电话统计
-- Describe：安全授权项用户电话统计
-- Input：upf.upf_cargo_call_driver_di
-- Input：upf.upf_cargo_shipper_df
-- dwd.user
-- ods.usercenterdb_uc_account
-- ods.usercenterdb_auth_item
-- ods.usercenterdb_auth_category
-- Output：dm_info.auth_item_usr_call_statistics
-- Author：李梦迪/Y0011067
-- Creation Date：20190318
-- Last Modified：
-- Change History：
---------------------------------------------------------------------


create table if not exists dm_info.auth_item_usr_call_statistics
(
	act_type                string      comment '统计类型，0.全量，1.日活，2.高活'   
   --,auth_code               bigint      comment '授权项码'
   ,auth_name               string      comment '授权项名称'
   --,auth_usr_cnt            bigint      comment '统计用户中授权的用户数'
   --,act_usr_cnt             bigint      comment '统计用户数'
   ,auth_rt					string		comment '已授权的用户占比'
   --,auth_usr_call_cnt            bigint      comment '统计用户中授权的用户电话数'
   --,act_usr_call_cnt             bigint      comment '统计用户电话数'
   ,call_rt					string		comment '已授权的用户的电话占比'
)
comment '安全授权项用户电话统计' 
partitioned by (day bigint comment '天分区')       
stored as parquet
;

insert overwrite table dm_info.auth_item_usr_call_statistics partition(day = ${zdt.addDay(-1).format("yyyyMMdd")})
select
    --auth.user_type, 
    auth.act_type
    --auth.auth_code,
    ,auth.name
    --auth.auth_usr_cnt,--授权用户数
    --act.act_usr_cnt,--活跃数
	,concat(cast(cast(auth.auth_usr_cnt/act.act_usr_cnt*100 as decimal(10,2)) as string),'%') 			as auth_rt
	--auth.auth_usr_call_cnt,--授权用户电话数
	--act.act_usr_call_cnt, --活跃电话数
	,concat(cast(cast(auth.auth_usr_call_cnt/act.act_usr_call_cnt*100 as decimal(10,2)) as string),'%') as call_rt
from
(
    select 
    	act.user_type
    	,act.act_type 
    	,usr.auth_code
    	,usr.name
    	,count(1) as auth_usr_cnt
		,sum(t1.call_cnt) as auth_usr_call_cnt
    from
    (
        select 
            user_id
            ,user_type
            ,0 as act_type
        from dwd.user
        where user_type=1
        and is_auth_success = 1 --20190318 lmd
        union all
        select
            user_id
            ,1 as user_type
            ,1 as act_type
        from
            upf.upf_cargo_call_driver_di
        where
            day = ${zdt.addDay(-1).format("yyyyMMdd")}
            and app_platform = 'MB'
            and call_cnt > 0
        
    	union all
    	select
    		user_id
    		,1 as user_type
    		,2 as act_type
    	from
    		upf.upf_cargo_call_driver_di
    	where
    		day between ${zdt.addDay(-30).format("yyyyMMdd")} and ${zdt.addDay(-1).format("yyyyMMdd")}
    		and app_platform = 'MB'
    		and call_cnt > 0
    	group by user_id
    	having(count(1) >= 10)	
    ) act
     join
    (
        select 
            acc.user_id
         	,acc.user_type
         	,auth.auth_code
         	,auth.name
         from
         (
          select 
          	account_id as user_id  
          	,account_type as user_type --用户类型
          	,tags
          from 
          	ods.usercenterdb_uc_account
          where
          	account_status = 1 --20190318 lmd
         ) acc
         inner join
        (
              select
             	t1.auth_code
             	,t1.name
             	,t2.account_type as user_type
              from
              	ods.usercenterdb_auth_item t1 
              left join ods.usercenterdb_auth_category t2 
              on t1.category_id = cast(t2.id as bigint)
              where
              	t1.is_valid=1
              	and t2.is_valid = 1
         ) auth
         --on (acc.tags&auth.auth_code = auth.auth_code) and acc.user_type = auth.user_type
         on 1 = 1
         where (acc.tags&auth.auth_code = auth.auth_code) and acc.user_type = auth.user_type
    ) usr
	on act.user_id = usr.user_id and act.user_type = usr.user_type
	join 
	(select
    		user_id
			,call_cnt
    	from
    		upf.upf_cargo_call_driver_di
    	where
    		day = ${zdt.addDay(-1).format("yyyyMMdd")}
			and app_platform = 'MB'
	) t1
	on usr.user_id=t1.user_id 
    group by  act.user_type, act.act_type, usr.auth_code, usr.name
) auth
left join
(	
	select 
		user_type
		,act_type
		,count(1) as act_usr_cnt
		,sum(call_cnt) as act_usr_call_cnt
	from 
        (select 
            user_id
            ,user_type
            ,0 as act_type
        from dwd.user
        where user_type=1
        and is_auth_success = 1 --20190318 lmd
        union all
        select
            user_id
            ,1 as user_type
            ,1 as act_type
        from
            upf.upf_cargo_call_driver_di
        where
            day = ${zdt.addDay(-1).format("yyyyMMdd")}
            and app_platform = 'MB'
            and call_cnt > 0
        
    	union all
    	select
    		user_id
    		,1 as user_type
    		,2 as act_type
    	from
    		upf.upf_cargo_call_driver_di
    	where
    		day between ${zdt.addDay(-30).format("yyyyMMdd")} and ${zdt.addDay(-1).format("yyyyMMdd")}
    		and app_platform = 'MB'
    		and call_cnt > 0
			group by user_id
			having(count(1) >= 10)
		) t1 
		join
		(select
    		user_id
			,call_cnt
    	from
    		upf.upf_cargo_call_driver_di
    	where
    		day = ${zdt.addDay(-1).format("yyyyMMdd")}
			and app_platform = 'MB'
		) t2
	    on t1.user_id=t2.user_id
    group by  t1.user_type, t1.act_type
) act
on auth.user_type = act.user_type and auth.act_type = act.act_type

