const Promise = require('bluebird');
const process = require('process');
const debug = require('debug')('init');
const db = require('sqlite');
const elasticsearch = require('elasticsearch');

const {
  cleanProducts,
  putProductsTemplate,
  queryProducts,
  indexProducts,
  collectProducts
} = require('./lib/products');
const {
  cleanOrders,
  putOrdersTemplate,
  queryOrders,
  collectOrders,
  indexOrders
} = require('./lib/orders');

const DB_FILE = 'db/instacart';

const init = () => {
  debug('instacart-es init');
  debug('connecting to Elasticsearch');

  const args = process.argv.slice(2);
  const host = (args.length) ? args[0] : 'localhost:9200';
  const es = new elasticsearch.Client({ apiVersion: '5.x', host });

  Promise.all([
    cleanProducts(es),
    cleanOrders(es),
    putProductsTemplate(es),
    putOrdersTemplate(es)
  ])
  .then(() => {
    debug(`opening SQLite database ${DB_FILE}`);
    return db.open(DB_FILE);
  })
  .then(() => {
    return queryProducts(db)
    .then(rows => {
      return Promise.all([
        indexProducts(rows, es),
        collectProducts(rows)
      ])
      .then(productData => productData[1]); // return the products collection
    });
  })
  .then(products => {
    return queryOrders(db)
    .then(rows => collectOrders(products, rows))
    .then(orders => indexOrders(orders, es));
  })
  .then(() => {
    debug(`closing SQLite database ${DB_FILE}`);
    return db.close();
  })
  .then(() => {
    console.log('Done!');
  })
  .catch(err => {
    console.log({ err });
  });
};

init();
