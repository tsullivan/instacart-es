const process = require('process');
const sqlite3 = require('sqlite3').verbose();
const elasticsearch = require('elasticsearch');
const productsTemplate = require('../templates/products');

const productsQuery = (`
select
  p.product_name as name,
  a.aisle as aisle,
  d.department as department
from products p
left outer join aisles a
  on p.aisle_id = a.aisle_id
left outer join departments d
  on p.department_id = d.department_id
limit 10
`);

const initProducts = (sql, es) => {
  es.indices.putTemplate({
    name: 'instacart_products',
    body: productsTemplate
  }).
  then(() => {
    sql.each(productsQuery, (err, row) => {
      if (err) {
        throw new Error(err);
      }
      const debug = [
        row.name,
        row.aisle,
        row.department,
      ];
      console.log(debug);
    });
  })
  .catch(err => {
    throw new Error(err);
  });
};

module.exports = () => {
  const sql = new sqlite3.Database('db/instacart');

  const args = process.argv.slice(2);
  const host = (args.length) ? args[0] : 'localhost:9200';
  const es = new elasticsearch.Client({ apiVersion: '5.4', host });

  initProducts(sql, es);
};
