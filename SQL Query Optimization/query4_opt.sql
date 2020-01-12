.timer on
select
	o_orderpriority,
	count(*) as order_count
from (	SELECT o_orderkey, o_orderpriority
	           FROM orders
	           WHERE   o_orderdate >= "1993-07-01" --'[DATE]'
	               and o_orderdate < "1993-10-01" --'[DATE]'
     )
where exists (
		select l_orderkey, l_commitdate, l_receiptdate
		from lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
			)
group by
	o_orderpriority
order by
	o_orderpriority;