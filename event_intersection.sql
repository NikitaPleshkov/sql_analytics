-- Problem:
-- Find the maximum number of concurrent events per day

-- We have
-- Table with id event and time borders

-------------------------------------------------------------------
-- DDL

-- drop database sql_analytics;
create database sql_analytics;

-- drop schema sql_test;
create schema sql_test;

-- drop table sql_test.event
create table sql_test.event (
  event_id serial primary key,
  begin_date timestamp,
  end_date timestamp
);
-------------------------------------------------------------------
-- generate data
insert into sql_test.event(event_id, begin_date, end_date)
select row_number() over () as event_id, begin_date,
       begin_date + random() * interval '4 hour' as end_date
from generate_series(now() - '3 day'::interval,
                      now(), '10 min') as begin_date;

-------------------------------------------------------------------
-- Solution:
-- -- Strategy:
-- -- Step 1. Select subset of events which have intersections
-- -- Step 2. Set begin and end point for subset (first and last event)
-- -- Step 3. Find interval which contains maximum count of events
--  -- -- Step 3.1 Find max(begin_date) and min(end_date)
--  -- -- Step 3.2 Sort max(begin_date) and min(end_date) as interval_start and interval_end


-- Calculate time intervals, which contains intersect of events
create view sql_test.event_intervals as
select begin_date, end_date, window_,
       least(max(begin_date) over (partition by window_), min(end_date) over (partition by window_)) as interval_start,
       greatest(max(begin_date) over (partition by window_), min(end_date) over (partition by window_)) as interval_end
from (select begin_date, end_date, border_num, first_value(border_num) over (partition by grp) as window_
      from (select begin_date, end_date, border_num,
                   sum(case when border_num is not null then 1 end) over (order by begin_date) as grp
            from (select begin_date, end_date,
                         case when border is not null
                              then row_number() over (partition by border order by begin_date)
                         end as border_num
                  from (select begin_date, end_date,
                               case when lag(is_window) over (order by begin_date) is null and is_window is not null
                                    then 'begin'
                                    when is_window is null then 'end' end as border
                        from (select begin_date, end_date,
                                     case when end_date between next_begin and next_end
                                               or next_end between begin_date and end_date
                                          then 1
                                     end as is_window
                              from (select begin_date, end_date,
                                           lead(begin_date) over (order by begin_date) as next_begin,
                                           lead(end_date) over (order by begin_date) as next_end
                                    from sql_test.event)
                              as src)
                        as cross_event)
                  as window_detection)
            as window_groupping)
      as window_numerate)
as result
order by begin_date;


-- -- Step 4. Select interval which contains maximum count of event per day
-- Calculate result
select distinct day_id, max(cnt) over (partition by day_id)
from (select count(1) as cnt, date_trunc('day', begin_date) as day_id, window_
      from sql_test.event_intervals
      group by day_id, window_) as result
order by day_id;
