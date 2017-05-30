const { identity, map } = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('orders');
const ordersTemplate = require('../templates/orders');
const { readFileSync } = require('fs');
const { clean, putTemplate } = require('./common');
const { divideBulks, flattenBulks } = require('./divide_bulks');
const { average } = require('./average');
const { Meter } = require('./meter');

const BULKS_LENGTH = 263; // (131209 / 500)
const INDEX = 'instacart_orders';

const cleanOrders = es => clean(es, INDEX);
const putOrdersTemplate = es => putTemplate(es, INDEX, ordersTemplate);

/*
 * Query for orders
 */
const queryOrders = db => {
  debug('querying orders');
  const queryOrdersMeter = new Meter('Querying orders');

  const ordersQuery = readFileSync('sql/select_orders.sql', 'utf8');
  return db.all(ordersQuery)
  .then(rows => {
    debug('found %d rows in db', rows.length);
    queryOrdersMeter.done();
    return rows;
  });
};

/*
 * Transform the collection of products and rows of orders
 * into a collection of orders keyed by order_id
 */
const collectOrders = (products, rows) => {
  debug(`received collection of ${Object.keys(products).length} products`);
  const createProductFromRow = row => {
    // find each product from the collection and add to the order
    return Object.assign({}, products[row.product_id], {
      add_to_cart_order: row.add_to_cart_order,
      reordered: row.reordered === 1
    });
  };
  const createInitialDoc = row => ({
    id: row.id,
    user_id: row.user_id,
    eval_set: row.eval_set,
    order_number: row.order_number,
    order_dow: row.order_dow,
    order_hour_of_day: row.order_hour_of_day,
    days_since_prior_order: row.days_since_prior_order,
    products: [ createProductFromRow(row) ] // nested
  });

  const orders = {};
  const collectOrdersMeter = new Meter('Collecting orders');
  rows.forEach(row => {
    if (orders[row.id]) {
      orders[row.id].products.push(createProductFromRow(row));
    } else {
      orders[row.id] = createInitialDoc(row);
    }
  });
  collectOrdersMeter.done();

  const productCounterMeter = new Meter('Counting products for each order');
  for (const orderId in orders) {
    const order = orders[orderId];
    // track the number of items in cart separately
    order.product_count = order.products.length;
  }
  productCounterMeter.done();

  const collection = map(orders, order => order);
  debug(
    'interpreted %d products in %d orders',
    collection.reduce((count, order) => (count + order.products.length), 0),
    collection.length
  );
  return collection;
};

/*
 * Upload each bulk set to ES
 */
const indexOrders = (orders, es) => {
  const bulks = divideBulks(orders, identity, BULKS_LENGTH, INDEX);
  debug('created orders in bulk sets, average length: %s', average(bulks.map(b => b.length)));

  const indexOrdersMeter = new Meter('Indexing orders');
  return Promise.each(bulks, bulk => {
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
  collectOrders,
  indexOrders
};
