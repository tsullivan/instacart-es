const { flatten } = require('lodash');
const Promise = require('bluebird');
const productsTemplate = require('../templates/products');
const log = require('./log');

const BULKS_LENGTH = 100;
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
  log('cleaning products');
  return es.indices.delete({
    index: 'instacart_products'
  })
  .catch(() => {
    log('no existing products, lol');
  });
};

const createDoc = row => ({
  id: row.id,
  name: row.name,
  aisle: row.aisle,
  department: row.department,
});

const indexProducts = (db, es) => {
  log('indexing products');
  return es.indices.putTemplate({
    name: 'instacart_products',
    body: productsTemplate
  })
  .then(() => {
    return db.all(productsQuery)
    .then(rows => {
      const bulks = [];
      for(let i = 0; i < BULKS_LENGTH; i++) {
        bulks[i] = [];
      }

      let rowIndex = rows.length - 1;
      let bulkIndex;
      while(rowIndex >= 0) {
        bulkIndex = rowIndex % BULKS_LENGTH;
        bulks[bulkIndex].push(createDoc(rows[rowIndex]));
        rowIndex--;
      }

      return bulks;
    });
  })
  .then(bulks => {
    return Promise.each(bulks, (bulk, index, length) => {
      log(`running products bulk request ${index + 1} of ${length}`);
      const body = flatten(bulk.map(b => [ { index: { _id: b.id } }, b ]));
      return es.bulk({
        index: 'instacart_products',
        type: 'doc',
        body
      });
    });
  });
};

module.exports = {
  cleanProducts,
  indexProducts
};
