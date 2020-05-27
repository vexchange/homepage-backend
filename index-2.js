const thorify = require("thorify").thorify;
const _ = require('lodash');
const Web3 = require("web3");
const fs = require('fs');

const config = require('./config');

const web3 = thorify(new Web3(), "http://localhost:8669");

(async () => {
  const vthoExchangeAddress = '0xf9F99f982f3Ea9020f0A0afd4D4679dFEe1B63cf';
  const lastUpdatedBlock = config.GENSIS_BLOCK_NUMBER;
  const exchange = new web3.eth.Contract(config.EXCHANGE_ABI, vthoExchangeAddress);

  for (const event in exchange.events) {
    console.log(event);
  }
})();
