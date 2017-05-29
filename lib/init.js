const Promise = require('bluebird');
const process = require('process');
const debug = require('debug')('init');
const db = require('sqlite');
const elasticsearch = require('elasticsearch');

const {
  cleanProducts,
  putProductsTemplate,
  queryProducts,
  indexProducts
} = require('./products');
const {
  cleanOrders,
  putOrdersTemplate,
  queryOrders,
  indexOrders
} = require('./orders');

const DB = 'db/instacart';

module.exports = () => {
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
    debug(`opening SQLite database ${DB}`);
    return db.open(DB);
  })
  .then(() => {
    return queryProducts(db)
    .then(bulks => indexProducts(bulks, es));
  })
  .then(() => {
    return queryOrders(db)
    .then(bulks => indexOrders(bulks, es));
  })
  .then(() => {
    debug(`closing SQLite database ${DB}`);
    return db.close();
  })
  .then(() => {
    console.log('Done!');
  })
  .catch(err => {
    console.log({ err });
  });
};
