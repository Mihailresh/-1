with order_price as 
(
select 
          a.order_id
        , a.creation_time
        , a.product_id
        , b.price

from(select 
              order_id
            , creation_time
            , unnest(product_ids) product_id
     from orders
     where order_id not in (select order_id
                            from user_actions
                            where action = 'cancel_order')
                            ) a
join products b
on a.product_id = b.product_id
)
,

-- считаем выручку с каждого не отмененного заказа

order_pr as 
(
select 
          order_id
        , sum(price) summ
from order_price
group by order_id
)
,

--Выручка, полученная в этот день

price_ord as 
(
select 
              creation_time::date as date
            , sum(price) revenue
            
    from order_price
    group by date
)
,

--находим новых пользователей по дням 

new_users_price as
(
select 
          time::date as date
        , user_id 
        , order_id
from
    (select *
            , rank() over (partition by user_id order by time::date) first_active
     from user_actions) t
where first_active = 1
)
,

-- выручка с новых пользователей 

sum_new_price_us as
(
select 
          a.date
        , sum(summ) new_users_revenue
from new_users_price a
join order_pr b 
on a.order_id = b.order_id
group by a.date
order by a.date
)
,

-- выручка с оставшихся пользователей

other_price as 
(
select 
          a.date
        , revenue - new_users_revenue other_revenue
from price_ord a 
join sum_new_price_us b 
on a.date = b.date
)

select 
          a.date
        , revenue
        , new_users_revenue
        , round(100 * new_users_revenue::decimal / revenue, 2) new_users_revenue_share
        , round(100 * other_revenue::decimal / revenue, 2) old_users_revenue_share
from price_ord a 
join sum_new_price_us b 
on a.date = b.date
join other_price c
on a.date = c.date
order by a.date
