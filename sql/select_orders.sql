select
  o.order_id as id,
  o.user_id as user_id,
  o.eval_set as eval_set,
  o.order_number as order_number,
  o.order_dow as order_dow,
  o.order_hour_of_day as order_hour_of_day,
  o.days_since_prior_order as days_since_prior_order,
  t.product_id as product_id,
  t.add_to_cart_order as add_to_cart_order,
  t.reordered as reordered
from order_products__train t
left outer join orders o
  on t.order_id = o.order_id
