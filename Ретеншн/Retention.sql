SELECT date_trunc('month', start_date)::date start_month ,
       start_date ,
       date - start_date day_number ,
       round(count(distinct user_id)::decimal / max(count(distinct user_id)) OVER (PARTITION BY start_date),
             2) retention
FROM   (SELECT user_id ,
               time::date as date ,
               min(time::date) OVER (PARTITION BY user_id) start_date
        FROM   user_actions) t1
GROUP BY start_date, date
ORDER BY start_date, day_number
