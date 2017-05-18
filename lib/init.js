const process = require('process');
const sqlite3 = require('sqlite3').verbose();
const elasticsearch = require('elasticsearch');
const productsTemplate = require('../templates/products');

const productsQuery = (`
select
  p.product_id as id,
  p.product_name as name,
  a.aisle as aisle,
  d.department as department
from products p
left outer join aisles a
  on p.aisle_id = a.aisle_id
left outer join departments d
  on p.department_id = d.department_id
limit 100
`);

const cleanProducts = (es) => {
  console.log('-- cleaning products --');
  return es.indices.delete({
    index: 'instacart_products'
  });
};
const initProducts = (sql, es) => {
  console.log('-- initting products --');
  return es.indices.putTemplate({
    name: 'instacart_products',
    body: productsTemplate
  })
  .then(() => {
    sql.each(productsQuery, (err, row) => {
      if (err) {
        throw new Error(err);
      }
      console.log(` - id:${row.id} - name:${row.name}`);
      const product = {
        id: row.id,
        name: row.name,
        aisle: row.aisle,
        department: row.department,
      };
      es.index({
        index: 'instacart_products',
        type: 'doc',
        body: product
      });
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
  const es = new elasticsearch.Client({ apiVersion: '5.x', host });

  cleanProducts(es)
  .then(() => {
    initProducts(sql, es);
  });
};
