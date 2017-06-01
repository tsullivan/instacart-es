const Promise = require('bluebird');
const debug = require('debug')('products');
const productsTemplate = require('../templates/products');
const { readFileSync } = require('fs');
const { clean, putTemplate } = require('./common');
const { divideBulks, flattenBulks } = require('./divide_bulks');
const { average } = require('./average');
const { Meter } = require('./meter');

const BULKS_LENGTH = 100; // (49688 / 500)
const INDEX = 'instacart_products';

const cleanProducts = es => clean(es, INDEX);
const putProductsTemplate = es => putTemplate(es, INDEX, productsTemplate);

/*
 * Query for products
 */
const queryProducts = db => {
  debug('querying orders');
  const queryProductsMeter = new Meter('Querying products');
  const productsQuery = readFileSync('sql/select_products.sql', 'utf8');
  return db.all(productsQuery)
  .then(rows => {
    debug('found %d rows in db', rows.length);
    queryProductsMeter.done();
    return rows;
  });
};

/*
 * Create an object of products keyed by product_id
 */
const collectProducts = rows => {
  const collectProductsMeter = new Meter('Collecting products');
  const collection = rows.reduce((coll, row) => {
    coll[row.id] = row;
    return coll;
  }, {});
  collectProductsMeter.done();
  debug(`collected ${Object.keys(collection).length} products`);
  return collection;
};

/*
 * Divide query result into multiple sets and bulk-upload each set to ES
 */
const indexProducts = (rows, es) => {
  const createDoc = row => ({
    id: row.id,
    name: row.name,
    aisle: row.aisle,
    department: row.department,
  });
  const bulks = divideBulks(rows, createDoc, BULKS_LENGTH, INDEX);
  debug('created products in bulk sets, average length: %s', average(bulks.map(b => b.length)));
  const indexProductsMeter = new Meter('Indexing products');

  return Promise.each(bulks, bulk => {
    return es.bulk({
      index: INDEX,
      type: 'doc',
      body: flattenBulks(bulk)
    });
  })
  .then(() =>  {
    indexProductsMeter.done();
    return bulks;
  });
};

module.exports = {
  cleanProducts,
  putProductsTemplate,
  queryProducts,
  indexProducts,
  collectProducts
};
