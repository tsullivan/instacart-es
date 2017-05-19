const { flatten } = require('lodash');

const divideBulks = (rows, createDoc, BULKS_LENGTH) => {
  const bulks = [];

  for(let i = 0; i < BULKS_LENGTH; i++) {
    bulks[i] = [];
  }

  let rowIndex = rows.length - 1;
  while(rowIndex >= 0) {
    bulks[rowIndex % BULKS_LENGTH].push(createDoc(rows[rowIndex]));
    rowIndex--;
  }

  return bulks;
};

const flattenBulks = bulk => flatten(bulk.map(b => [ { index: { _id: b.id } }, b ]));

module.exports = { divideBulks, flattenBulks };
