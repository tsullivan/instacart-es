const process = require('process');
const db = require('sqlite');
const elasticsearch = require('elasticsearch');
const { cleanProducts, indexProducts } = require('./products');

module.exports = () => {
  const args = process.argv.slice(2);
  const host = (args.length) ? args[0] : 'localhost:9200';
  const es = new elasticsearch.Client({ apiVersion: '5.x', host });

  Promise.resolve()
  .then(() => cleanProducts(es))
  .then(() => db.open('db/instacart'))
  .then(() => indexProducts(db, es));
};
