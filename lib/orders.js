const Promise = require('bluebird');
const log = require('./log');
const ordersTemplate = require('../templates/orders');
const { readFileSync } = require('fs');
const { divideBulks, flattenBulks } = require('./divide_bulks');
const { identity, map } = require('lodash');
const debug = require('debug')('insta');
const Meter = require('cli-meter');
const process = require('process');

const BULKS_LENGTH = 277;

const cleanOrders = (es) => {
  log('cleaning orders');
  return es.indices.delete({ index: 'instacart_orders' })
  .catch(() => log('no existing orders, lol'));
};

const indexOrders = (db, es) => {
  log('indexing orders');

  return Promise.resolve()
  .then(() => {
    return es.indices.putTemplate({
      name: 'instacart_orders',
      body: ordersTemplate
    });
  })
  .then(() => {
    const ordersQuery = readFileSync('sql/select_orders.sql', 'utf8');
    const createInitialDoc = (row) => ({
      id: row.id,
      user_id: row.user_id,
      eval_set: row.eval_set,
      order_number: row.order_number,
      order_dow: row.order_dow,
      order_hour_of_day: row.order_hour_of_day,
      days_since_prior_order: row.days_since_prior_order,
      products: [ { id: row.product_id } ] // nested
    });
    return db.all(ordersQuery)
    .then(rows => {
      debug(`found ${rows.length} rows in db`);
      const orders = {};
      const m = new Meter({ total: rows.length});
      rows.forEach(row => {
        m.step(1);
        process.stdout.write(`Collecting orders ${m}\n`);
        process.stdout.moveCursor(0, -1);

        if (orders[row.id]) {
          debug(`found order in cache: ${row.id}`);
          orders[row.id].products.push(row.product_id);
        } else {
          debug(`add new initial order to cache: ${row.id}`);
          orders[row.id] = createInitialDoc(row);
        }
        debug(`products in order ${row.id}: [ ${JSON.stringify(orders[row.id].products)} ]`);
      });
      process.stdout.moveCursor(0, 1);

      return map(orders, order => order);
    })
    .then(orders => {
      const numProducts = orders.reduce((count, order) => (count + order.products.length), 0);
      debug(`interpreted ${numProducts} in ${orders.length} orders`);
      const bulks = divideBulks(orders, identity, BULKS_LENGTH);
      debug(`created a set of bulks ${bulks.map(b => b.length)}`);
      const m = new Meter({ total: bulks.length});

      return Promise.each(bulks, bulk => {
        m.step(1);
        process.stdout.write(`Indexing orders ${m}\n`);
        process.stdout.moveCursor(0, -1);

        return es.bulk({
          index: 'instacart_orders',
          type: 'doc',
          body: flattenBulks(bulk)
        });
      });
    })
    .then(() => {
      process.stdout.moveCursor(0, 1);
    });
  });
};

module.exports = { cleanOrders, indexOrders };
