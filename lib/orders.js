const { countBy, identity, map } = require('lodash');
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
 * into a collection of orders keyed by order_id.
 * Finds each product from the collection and adds to the orders
 */
const collectOrders = (products, rows) => {
  debug(`received collection of ${Object.keys(products).length} products`);
  const collectOrdersMeter = new Meter('Collecting orders');

  const orders = {};
  const createInitialDoc = row => ({
    id: row.id,
    user_id: row.user_id,
    eval_set: row.eval_set,
    order_number: row.order_number,
    order_dow: row.order_dow,
    order_hour_of_day: row.order_hour_of_day,
    days_since_prior_order: row.days_since_prior_order,
    product_names: [],
    product_details: []
  });
  const createProductDetailFromRow = (row, product) => {
    return Object.assign({}, product, {
      add_to_cart_order: row.add_to_cart_order,
      reordered: row.reordered === '1'
    });
  };
  // populate product name and detail info for orders
  for (const rowIndex in rows) {
    const row = rows[rowIndex];
    const product = products[row.product_id];
    const order = orders[row.id] || createInitialDoc(row, product);
    if (!orders[row.id]) { orders[row.id] = order; }
    order.product_names.push(product.name);
    order.product_details.push(createProductDetailFromRow(row, product));
  }
  for (const orderId in orders) {
    const order = orders[orderId];
    // populate product metric info for orders
    const products_total = order.product_details.length;
    const products_reorders = countBy(order.product_details, p => p.reordered)['true'] || 0;
    const products_neworders = products_total - products_reorders;
    // populate product categorization info for orders
    const products_aisles = order.product_details.map(p => p.aisle);
    const products_departments = order.product_details.map(p => p.department);

    Object.assign(order, {
      products_total,
      products_reorders,
      products_neworders,
      products_aisles,
      products_departments
    });
  }
  collectOrdersMeter.done();

  const collection = map(orders, order => order);
  debug(
    'interpreted %d products in %d orders',
    collection.reduce((count, order) => (count + order.product_details.length), 0),
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
