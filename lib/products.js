const Promise = require('bluebird');
const productsTemplate = require('../templates/products');
const { readFileSync } = require('fs');
const { divideBulks, flattenBulks } = require('./divide_bulks');
const Meter = require('cli-meter');
const process = require('process');

const BULKS_LENGTH = 100;

const cleanProducts = (es) => {
  return es.indices.delete({ index: 'instacart_products' })
  .catch(() => {});
};

const indexProducts = (db, es) => {
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
    const m = new Meter({ total: bulks.length});

    return Promise.each(bulks, bulk => {
      m.step(1);
      process.stdout.write(`Indexing products ${m}\n`);
      process.stdout.moveCursor(0, -1);

      return es.bulk({
        index: 'instacart_products',
        type: 'doc',
        body: flattenBulks(bulk)
      });
    })
    .then(() => {
      process.stdout.moveCursor(0, 1);
    });
  });
};

module.exports = { cleanProducts, indexProducts };
