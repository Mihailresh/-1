-- Шаг 1. Узнаем, когда была первая транзакция для каждого студента

with first_payments as
(
select    user_id
        , min(date_trunc('day', transaction_datetime)) first_payment_date
from skyeng_db.payments
where status_name = 'success' and id_transaction is not null
group by user_id
)
,

-- Шаг 2. Соберем таблицу с датами за каждый календарный день 2016 года

all_dates as 
(
select distinct class_end_datetime::date as dt
from skyeng_db.classes 
where date_trunc('year', class_start_datetime) = '2016-01-01'
)
,

-- Шаг 3. Узнаем, за какие даты имеет смысл собирать баланс для каждого студента

all_dates_by_user as
(
select    user_id
        , dt
from all_dates a
join first_payments b
on a.dt >= b.first_payment_date
)
,

-- Шаг 4. Найдем все изменения балансов, связанные с успешными транзакциями

payments_by_dates as
(
select    
          user_id
        , date_trunc('day', transaction_datetime) as payment_date
        , sum(classes) transaction_balance_change
from skyeng_db.payments
where status_name = 'success' and id_transaction is not null
group by 1,2
)
,

-- Шаг 5. Найдем баланс студентов, который сформирован только транзакциями

payments_by_dates_cumsum as 
(
select    b.user_id
        , b.dt
        , transaction_balance_change
        , sum(coalesce(transaction_balance_change, 0))over(partition by b.user_id order by b.dt) transaction_balance_change_cs
from all_dates_by_user b 
left join payments_by_dates a
        on a.user_id = b.user_id 
        and a.payment_date = b.dt
)
, 

-- Шаг 6. Найдем изменения балансов из-за прохождения уроков

classes_by_dates as 
(
select    user_id
        , class_end_datetime::date as class_date
        , count(id_class) * -1 as classes
from skyeng_db.classes
where class_type <> 'trial'
and class_status in ('success', 'failed_by_student')
group by 1,2 
)
,

-- Шаг 7. По аналогии с уже проделанным шагом для оплат создадим CTE для хранения кумулятивной суммы количества пройденных уроков

classes_by_dates_dates_cumsum as
(
select    b.user_id
        , b.dt
        , classes
        , sum(coalesce(classes, 0))over(partition by b.user_id order by b.dt) classes_cs
from all_dates_by_user b
left join classes_by_dates a
        on a.user_id = b.user_id 
        and a.class_date = b.dt
)
,

--Шаг 8. Создадим CTE balances с вычисленными балансами каждого студента

balances as
(
select    a.user_id
        , a.dt
        , transaction_balance_change
        , transaction_balance_change_cs
        , classes
        , classes_cs
        , classes_cs + transaction_balance_change_cs as balance
from classes_by_dates_dates_cumsum a
join payments_by_dates_cumsum b
on a.user_id = b.user_id 
and a.dt = b.dt
)

select 
          dt 
        , sum(transaction_balance_change) sum_transaction_balance_change
        , sum(transaction_balance_change_cs) sum_transaction_balance_change_cs
        , sum(classes) sum_classes 
        , sum(classes_cs) sum_classes_cs 
        , sum(balance) sum_balance
from balances
group by dt
order by dt
