const Promise = require('bluebird');
const productsTemplate = require('../templates/products');
const { readFileSync } = require('fs');
const { divideBulks, flattenBulks } = require('./divide_bulks');
const log = require('./log');

const BULKS_LENGTH = 100;

const cleanProducts = (es) => {
  log('cleaning products');
  return es.indices.delete({ index: 'instacart_products' })
  .catch(() => log('no existing products, lol'));
};

const indexProducts = (db, es) => {
  log('indexing products');
  return Promise.resolve()
  .then(() => {
    return es.indices.putTemplate({
      name: 'instacart_products',
      body: productsTemplate
    });
  })
  .then(() => {
    const productsQuery = readFileSync('sql/select_products.sql', 'utf8');
    const createDoc = row => ({
      id: row.id,
      name: row.name,
      aisle: row.aisle,
      department: row.department,
    });
    return db.all(productsQuery)
    .then(rows => divideBulks(rows, createDoc, BULKS_LENGTH));
  })
  .then(bulks => {
    return Promise.each(bulks, (bulk, index, length) => {
      log(`running products bulk request ${index + 1} of ${length}`);
      return es.bulk({
        index: 'instacart_products',
        type: 'doc',
        body: flattenBulks(bulk)
      });
    });
  });
};

module.exports = { cleanProducts, indexProducts };
