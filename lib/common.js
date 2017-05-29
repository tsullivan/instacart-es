const debug = require('debug')('common');

const clean = (es, index) => {
  debug(`checking if ${index} index exists`);
  return es.indices.exists({ index })
  .then(exists => {
    debug('%s exists: %s', index, exists);
    if (exists) {
      debug(`deleting existing ${index} index`);
      return es.indices.delete({ index });
    }
  });
};

const putTemplate = (es, index, template) => {
  debug(`putting ${index} template`);
  return es.indices.putTemplate({ name: index, body: template });
};

module.exports = { clean, putTemplate };
