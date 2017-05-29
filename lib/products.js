const Promise = require('bluebird');
const debug = require('debug')('products');
const productsTemplate = require('../templates/products');
const { readFileSync } = require('fs');
const { clean, putTemplate } = require('./common');
const { divideBulks, flattenBulks } = require('./divide_bulks');
const { Meter } = require('./meter');

const BULKS_LENGTH = 100; // (49688 / 500)
const INDEX = 'instacart_products';

const cleanProducts = es => clean(es, INDEX);
const putProductsTemplate = es => putTemplate(es, INDEX, productsTemplate);

const queryProducts = db => {
  debug('querying orders');
  const productsQuery = readFileSync('sql/select_products.sql', 'utf8');
  const createDoc = row => ({
    id: row.id,
    name: row.name,
    aisle: row.aisle,
    department: row.department,
  });

  return db.all(productsQuery)
  .then(rows => {
    debug('found %d rows in db', rows.length);
    return divideBulks(rows, createDoc, BULKS_LENGTH, INDEX);
  });
};

const indexProducts = (bulks, es) => {
  debug('received products in bulk sets %s', bulks.map(b => b.length));
  const indexProductsMeter = new Meter('Indexing products', bulks.length);

  return Promise.each(bulks, bulk => {
    indexProductsMeter.step(1);
    return es.bulk({
      index: INDEX,
      type: 'doc',
      body: flattenBulks(bulk)
    });
  })
  .then(() =>  {
    indexProductsMeter.done();
  });
};

module.exports = {
  cleanProducts,
  putProductsTemplate,
  queryProducts,
  indexProducts
};
