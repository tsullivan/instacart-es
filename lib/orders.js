const { identity, map } = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('orders');
const ordersTemplate = require('../templates/orders');
const { readFileSync } = require('fs');
const { clean, putTemplate } = require('./common');
const { divideBulks, flattenBulks } = require('./divide_bulks');
const { Meter } = require('./meter');

const BULKS_LENGTH = 263; // (131209 / 500)
const INDEX = 'instacart_orders';

const cleanOrders = es => clean(es, INDEX);
const putOrdersTemplate = es => putTemplate(es, INDEX, ordersTemplate);

const queryOrders = db => {
  debug('querying orders');
  const ordersQuery = readFileSync('sql/select_orders.sql', 'utf8');
  const createInitialDoc = row => ({
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
    debug('found %d rows in db', rows.length);
    const orders = {};
    const collectOrdersMeter = new Meter('Collecting orders', rows.length);
    rows.forEach(row => {
      collectOrdersMeter.step(1);
      if (orders[row.id]) {
        orders[row.id].products.push({ id: row.product_id });
      } else {
        orders[row.id] = createInitialDoc(row);
      }
    });

    collectOrdersMeter.done();
    return map(orders, order => order);
  })
  .then(orders => {
    debug(
      'interpreted %d products in %d orders',
      orders.reduce((count, order) => (count + order.products.length), 0),
      orders.length
    );
    return divideBulks(orders, identity, BULKS_LENGTH, INDEX);
  });
};

const indexOrders = (bulks, es) => {
  debug('received orders in bulk sets %s', bulks.map(b => b.length));
  const indexOrdersMeter = new Meter('Indexing orders', bulks.length);

  return Promise.each(bulks, bulk => {
    indexOrdersMeter.step(1);
    return es.bulk({
      index: INDEX,
      type: 'doc',
      body: flattenBulks(bulk)
    });
  })
  .then(() => {
    indexOrdersMeter.done();
  });
};

module.exports = {
  cleanOrders,
  putOrdersTemplate,
  queryOrders,
  indexOrders
};
