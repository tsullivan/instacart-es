const Promise = require('bluebird');
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
`);

const cleanProducts = (es) => {
  console.log('-- cleaning products --');
  return es.indices.delete({
    index: 'instacart_products'
  })
  .catch(() => {
    console.log('-- no existing products, lol --');
  });
};

const createDoc = row => ({
  id: row.id,
  name: row.name,
  aisle: row.aisle,
  department: row.department,
});

const initProducts = (db, es) => {
  console.log('-- initting products --');
  return es.indices.putTemplate({
    name: 'instacart_products',
    body: productsTemplate
  })
  .then(() => {
    db.all(productsQuery)
    .then(rows => {
      console.log({ rowsLength: rows.length });
      return Promise.each(rows, row => {
        const body = createDoc(row);
        console.log({ body });
        return es.index({
          index: 'instacart_products',
          type: 'doc',
          body
        });
      });
    });
  });
};

module.exports = {
  cleanProducts,
  initProducts
};
