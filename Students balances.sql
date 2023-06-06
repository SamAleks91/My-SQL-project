with first_payments as (
select distinct user_id
      ,date_trunc('day',first_value(transaction_datetime) over (partition by user_id order by transaction_datetime)) as first_payment_date
from skyeng_db.payments
where status_name = 'success'
),
-----------------------------------------------------------
all_dates as (
select distinct date_trunc('day', class_end_datetime) as dt
from skyeng_db.classes 
where date_trunc('year', class_end_datetime) = '2016-01-01'
order by dt

),
------------------------------------------------------------
all_dates_by_user as (
select  t2.user_id
       ,t1.dt
from all_dates t1 
   inner join first_payments t2
      on t1.dt >= t2.first_payment_date 
)
--select * from all_dates_by_user where user_id = 16715252
,
------------------------------------------------------------
payments_by_dates as (
select user_id
      ,date_trunc('day', transaction_datetime) as payment_date
      ,sum(classes) as transaction_balance_change
from skyeng_db.payments
where status_name = 'success' 
group by user_id, payment_date
order by user_id, payment_date
),
------------------------------------------------------------
payments_by_dates_cumsum as (
select 
      t1.user_id
   --  ,t1.payment_date
     ,t1.dt
     ,transaction_balance_change
     ,sum(coalesce(transaction_balance_change,0)) over (partition by t1.user_id order by t1.dt rows between unbounded preceding and current row) as transaction_balance_change_cs 
from all_dates_by_user  t1
left join payments_by_dates t2
        on t1.user_id = t2.user_id 
        and t1.dt = t2.payment_date
)
--select * from payments_by_dates_cumsum where transaction_balance_change_cs = 0
------------------------------------------------------------
,
classes_by_dates as (
select
      user_id
     ,date_trunc('day', class_end_datetime) as class_date
     ,count(*) * (-1) as classes
from skyeng_db.classes
where class_type != 'trial'
      and (class_status = 'success' or class_status = 'failed_by_student')
group by user_id, class_date 
),
------------------------------------------------------------
classes_by_dates_dates_cumsum as (
select t1.user_id
--      ,t1.class_date
      ,t1.dt
      ,t2.classes
      ,sum(coalesce(t2.classes,0)) over (partition by t1.user_id order by t1.dt asc) as classes_cs
from all_dates_by_user  t1
left join classes_by_dates t2 
       on t1.user_id = t2.user_id
       and t1.dt  = t2.class_date
)
--select * from classes_by_dates_dates_cumsum where classes_cs is null
------------------------------------------------------------
,
balances as (
select t1.user_id
      ,t1.dt
      ,transaction_balance_change
      ,transaction_balance_change_cs
      ,classes
      ,classes_cs
      ,classes_cs + transaction_balance_change_cs as balance
from payments_by_dates_cumsum t1
inner join classes_by_dates_dates_cumsum t2
        on t1.user_id = t2.user_id
        and t1.dt = t2.dt
)
-- select * from balances
-- order by user_id, dt
-- limit 1000

------------------------------------------------------------
select 
     dt
     ,sum(transaction_balance_change) as sum_transaction_balance_change
     ,sum(transaction_balance_change_cs) as cssum_transaction_balance_change
     ,sum(classes) as sum_classes
     ,sum(classes_cs) as cs_classes
     ,sum(balance) as sum_balance
from balances 
group by dt
order by dt