const { flatten } = require('lodash');
const { Meter } = require('./meter');

const divideBulks = (rows, createDoc, length, index) => {
  const m = new Meter(`Creating ${length} bulk requests for ${index}`);
  const bulks = [];

  for(let i = 0; i < length; i++) {
    bulks[i] = [];
  }

  let rowIndex = rows.length - 1;
  while(rowIndex >= 0) {
    bulks[rowIndex % length].push(createDoc(rows[rowIndex]));
    rowIndex--;
  }
  m.done();

  return bulks;
};

const flattenBulks = bulk => flatten(bulk.map(b => [ { index: { _id: b.id } }, b ]));

module.exports = { divideBulks, flattenBulks };
