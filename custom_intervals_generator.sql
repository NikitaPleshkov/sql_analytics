-- Problem:
-- -- We need to generate specific time intervals with custom label for using in front analytics
-- We have
-- -- 2 dates (start and end of interval)
-- -- Some logic:
-- -- -- We need to use only last 7 year
-- -- -- If the difference between start and end >= 1 and <= 7 - dimension is day
-- -- -- If the difference between start and end > 7 and <= 49 - dimension is 7 day (week)
-- -- -- If the difference between start and end > 49 and <= 210 - dimension is month
-- -- -- If the difference between start and end > 210 and <= 540 - dimension is quarter
-- -- -- If the difference between start and end > 540 and <= 2160 - dimension is year
-- -- -- We need to have ability for using custom label

-- Solution:
-- -- Solution based on postgresql function with dialect plpgsql
-- -- But if we need, we can adaptive this solution to another DB with minimal fix

-- drop function sql_test.intervals_generator(date, date);
create or replace function sql_test.intervals_generator(date_begin date default (now() - '6 year'::interval + '1 day'::interval)::date,
	                                                      date_end date default ('now'::text)::date)
	returns table(label text, begin_date date, end_date date)
	language plpgsql
as $$
declare delta interval;
declare d_min date;
declare d_max date;
begin
-- Initialize constants
set lc_time = 'ru_RU.UTF8';
-- For error with time borders
d_min = least(date_begin, date_end);
d_max = greatest(date_begin, date_end);
date_begin = d_min;
date_end = d_max;
-- Initialize date_begin according to problem
date_begin = (select case when (date_end-date_begin+1)>2160
                          then (date_end-'6 year'::interval+'1 day'::interval)::date
			  else date_begin
		     end);
-- Intialize delta according to problem
delta = (select case when (date_end-date_begin+1)<=7 and (date_end-date_begin+1)>=1 then '1 day'::interval
          	     when (date_end-date_begin+1)<=49 and (date_end-date_begin+1)>7 then '7 day'::interval
          	     when (date_end-date_begin+1)<=210 and (date_end-date_begin+1)>49 then '1 month'::interval
          	     when (date_end-date_begin+1)<=540 and (date_end-date_begin+1)>210 then '3 month'::interval
          	     when (date_end-date_begin+1)<=2160 and (date_end-date_begin+1)> 540 then '1 year'::interval
          	     else '1 year'::interval
                end);
-- Add 1 day to end point for logic below
date_end = (select date_end + '1 day'::interval);
-- Main functional
return query(
    with recursive recur as (
    -- According to delta chose start and end of iteration
        select date_begin as  iter,
	       case when delta = '1 day'::interval then ((date_trunc('day', date_begin))::date + delta)::date
		    when delta = '7 day'::interval then ((date_trunc('week', date_begin))::date + delta)::date
		    when delta = '1 month'::interval then ((date_trunc('month', date_begin))::date + delta)::date
		    when delta = '3 month'::interval then ((date_trunc('quarter', date_begin))::date + delta)::date
		    when delta = '1 year'::interval then ((date_trunc('year', date_begin))::date + delta)::date
	       end as iter_end
	union
	select case when delta = '1 day'::interval then ((date_trunc('day', iter))::date + delta)::date
		    when delta = '7 day'::interval then ((date_trunc('week', iter))::date + delta)::date
		    when delta = '1 month'::interval then ((date_trunc('month', iter))::date + delta)::date
		    when delta = '3 month'::interval then ((date_trunc('quarter', iter))::date + delta)::date
		    when delta = '1 year'::interval then ((date_trunc('year', iter))::date + delta)::date
	       end as iter,
	       -- Check exit from recursive
	       case when (iter_end+delta)::date > date_end then date_end::date
	            else (iter_end+delta)::date
	       end as iter_end
	from recur 
        where iter <=(date_end-delta-'1 day'::interval)::date)
	-- Select data from recursive and set custom label logic if we need
    select case when delta = '1 day' then to_char(iter, 'DD.MM')
                when delta = '7 day' then concat('Нед. '::text, extract('week' from iter::date))
                when delta = '1 month' then to_char(iter, 'TMMon')
                when delta = '3 month' then concat(to_char(iter,'Q'), ' кв. ', to_char(iter,'YYYY'))
                when delta = '1 year' then to_char(iter,'YYYY')
           end as time_line,
	   -- If you need, you can include some logic functional here									       
	   iter::date,
           iter_end::date
    from recur
    group by iter, iter_end
    order by iter);
end
$$
;
select * from sql_test.intervals_generator('2013-12-31', '2013-12-01');
-- Next steps:
-- -- We can to use generate_series or another function 
