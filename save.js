const fs = require('fs');

const lastBlock = block => {
  fs.writeFileSync('lastBlock.json', JSON.stringify({ block }));
};

module.exports = {
  lastBlock,
}
