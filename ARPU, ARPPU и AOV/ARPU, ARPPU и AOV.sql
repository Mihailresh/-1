with order_price as (SELECT a.order_id ,
                            a.creation_time ,
                            a.product_id ,
                            b.price FROM(SELECT order_id ,
                                         creation_time ,
                                         unnest(product_ids) product_id
                                  FROM   orders
                                  WHERE  order_id not in (SELECT order_id
                                                          FROM   user_actions
                                                          WHERE  action = 'cancel_order')) a join products b
                             ON a.product_id = b.product_id) , sum_day as (SELECT date ,
                                                     sum(revenu) OVER (ORDER BY date) revenue
                                              FROM   (SELECT creation_time::date as date ,
                                                             sum(price) revenu
                                                      FROM   order_price
                                                      GROUP BY date) tt1) , count_us as (SELECT date ,
                                          sum(count_userss) OVER (ORDER BY date) count_users
                                   FROM   (SELECT first_active_time_us::date as date ,
                                                  count(distinct user_id) count_userss
                                           FROM   (SELECT * ,
                                                          min(time) OVER (PARTITION BY user_id) first_active_time_us
                                                   FROM   user_actions
                                                   ORDER BY time) t
                                           GROUP BY date) tt2) , count_pay_us as (SELECT date ,
                                              sum(count_pay_user) OVER (ORDER BY date) count_pay_users
                                       FROM   (SELECT first_active_time_us::date as date ,
                                                      count(distinct user_id) count_pay_user
                                               FROM   (SELECT * ,
                                                              min(time) OVER (PARTITION BY user_id) first_active_time_us
                                                       FROM   user_actions
                                                       WHERE  order_id not in (SELECT order_id
                                                                               FROM   user_actions
                                                                               WHERE  action = 'cancel_order')
                                                       ORDER BY time) t
                                               GROUP BY date) tt) , count_orders as (SELECT date ,
                                             sum(count_or) OVER (ORDER BY date) count_ord
                                      FROM   (SELECT creation_time::date as date ,
                                                     count(order_id) count_or
                                              FROM   orders
                                              WHERE  order_id not in (SELECT order_id
                                                                      FROM   user_actions
                                                                      WHERE  action = 'cancel_order')
                                              GROUP BY date) t)
SELECT a.date ,
       round(a.revenue::decimal / b.count_users, 2) running_arpu ,
       round(a.revenue::decimal / c.count_pay_users, 2) running_arppu ,
       round(a.revenue::decimal / d.count_ord, 2) running_aov
FROM   sum_day a join count_us b
        ON a.date = b.date join count_pay_us c
        ON a.date = c.date join count_orders d
        ON a.date = d.date
