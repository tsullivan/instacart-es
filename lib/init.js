const process = require('process');
const sqlite3 = require('sqlite3').verbose();
const elasticsearch = require('elasticsearch');

const { cleanProducts, initProducts } = require('./products');

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
