with order_price as 
(
select 
          a.order_id
        , a.creation_time
        , a.product_id
        , b.price
        , b.name
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

-- рассчитываем сумму НДС

nds_sum as 
(
select 
          creation_time::date as date
        , sum(nds) tax
from
    (select 
              *
            , case 
                when name in ('сахар', 'сухарики', 'сушки', 'семечки','масло льняное', 'виноград', 'масло оливковое','арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 
                              'овсянка', 'макароны', 'баранина', 'апельсины','бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая','мука', 'шпроты', 'сосиски', 
                              'свинина', 'рис','масло кунжутное', 'сгущенка', 'ананас', 'говядина','соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки', 'груши',
                              'лепешка', 'молоко', 'курица', 'лаваш', 'вафли', 'мандарины') then round(price * 0.1/1.1, 2) 
                                                                                            else round(price * 0.2/1.2, 2)
                                                                                            end nds
    from order_price) t
group by date
)
,

-- выручка, полученная в этот день.

price_ord as 
(
select 
              creation_time::date as date
            , sum(price) revenue
    from order_price
    group by date
)
,

-- затраты на курьеров, у которых более 5 заказов за день, по дням группировка

count_orders_couriers as 
(
select 
          date
        , sum(cr_costs) cr_day_costs
from
    (select 
              t.*
            , case
                when date_part('month', date) = 8 and date_part('year', date) = 2022 and count_cr >= 5 then 400 
                when date_part('month', date) = 9 and date_part('year', date) = 2022 and count_cr >= 5 then 500
                                                                                                       else 0 end cr_costs
    from
        (select 
                  time::date as date
                , courier_id
                , count(courier_id) count_cr 
        from courier_actions
        where order_id not in (select order_id
                                    from user_actions
                                    where action = 'cancel_order')
        and action = 'deliver_order'
        group by date, courier_id) t 
        ) tt
group by date
order by date
)
,

-- постоянные ежедневные затраты

fixed_costs as 
(
select 
          distinct time::date as date
        , case
            when date_part('month', time) = 8 and date_part('year', time) = 2022 then 120000
            when date_part('month', time) = 9 and date_part('year', time) = 2022 then 150000
                                                                                   end fixed_cost
from courier_actions
order by date
)
,

--затраты на доставленный заказ и обертка заказа

orders_cost as
(
select 
          distinct time::date as date 
        , sum(fixed_cost) as delivery_pack_sum
from
    (select 
              *
            , case
                when date_part('month', time) = 8 and date_part('year', time) = 2022 and action = 'accept_order' then 140
                when date_part('month', time) = 9 and date_part('year', time) = 2022 and action = 'accept_order' then 115
                when date_part('month', time) = 8 and date_part('year', time) = 2022 and action = 'deliver_order' then 150
                when date_part('month', time) = 9 and date_part('year', time) = 2022 and action = 'deliver_order' then 150
                                                                                       end fixed_cost
    from courier_actions
    where order_id not in (select order_id
                                from user_actions
                                where action = 'cancel_order')
                                ) t
group by date
order by date
)
,

-- суммарные затраты 

sum_costs as 
(
select 
          a.date
        , cr_day_costs + fixed_cost + delivery_pack_sum costs
from count_orders_couriers a 
join fixed_costs b
on a.date = b.date
join orders_cost c
on a.date = c.date
order by a.date
)

select 
          *
        , round(100 * total_gross_profit::decimal / total_revenue, 2) total_gross_profit_ratio
from 
    (select 
              *
            , sum(gross_profit) over (order by date) total_gross_profit
            , round(100 * gross_profit::decimal / revenue, 2) gross_profit_ratio
    from
        (select 
                  a.date
                , a.revenue
                , b.costs
                , c.tax
                , a.revenue - b.costs - c.tax gross_profit
                , sum(a.revenue) over (order by a.date) total_revenue
                , sum(b.costs) over (order by a.date) total_costs
                , sum(c.tax) over (order by a.date) total_tax
        from price_ord a 
        join sum_costs b 
        on a.date = b.date
        join nds_sum c
        on a.date = c.date) t
        ) tt
