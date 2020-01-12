.timer on
select
	l_orderkey,
	sum(l_extendedprice*(1-l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from (	SELECT o_orderkey, o_orderdate, o_shippriority, o_custkey
	FROM orders
	NOT INDEXED
	WHERE o_orderdate < "1995-03-15" --'[DATE]'
)
CROSS JOIN ( SELECT c_custkey
	FROM customer 
	WHERE c_mktsegment = 'BUILDING' --'[SEGMENT]'
) ON c_custkey = o_custkey
CROSS JOIN (	SELECT l_orderkey, l_extendedprice, l_discount
	FROM lineitem
	WHERE l_shipdate > "1995-03-15" --'[DATE]'
) ON l_orderkey = o_orderkey
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;