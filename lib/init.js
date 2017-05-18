const sqlite = require('sqlite3').verbose();

const productsQuery = (`
select
  p.product_name as product_name,
  a.aisle as aisle,
  d.department as department
from products p
left outer join aisles a
  on p.aisle_id = a.aisle_id
left outer join departments d
  on p.department_id = d.department_id
limit 10
`);

const init = () => {
  var db = new sqlite.Database('db/instacart');
  db.each(productsQuery, (err, row) => {
    if (err) {
      throw new Error(err);
    }
    const debug = [
      row.product_name,
      row.aisle,
      row.department,
    ]
    console.log(debug);
  });
};

module.exports = init();
