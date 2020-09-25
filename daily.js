require('dotenv').config();

const Web3 = require("web3");
const thorify = require("thorify").thorify;
const _ = require('lodash');
const async = require("async");
const moment = require('moment');
const fs = require('fs');
const Framework = require('@vechain/connex-framework').Framework;
const ConnexDriver = require('@vechain/connex-driver');
const ethers = require('ethers').ethers;
const axios = require('axios');
const abi = require('thor-devkit').abi;

const NODE_URL = process.env.NODE_URL;

const web3 = thorify(new Web3(), NODE_URL);

const config = require('./config');

const vexchangeFactory = new web3.eth.Contract(
  config.FACTORY_ABI,
  config.FACTORY_ADDRESS,
);

const handler = {
  get: function(target, name) {
    return target.hasOwnProperty(name) ? target[name] : 0;
  }
};

const filterObject = (obj, predicate) => {
  return Object.keys(obj)
    .filter(key => predicate(obj[key]))
    .reduce((res, key) => (res[key] = obj[key], res), {})
};

(async () => {
  const { Driver, SimpleNet } = ConnexDriver;

  const driver = await Driver.connect(new SimpleNet(NODE_URL));
  const connex = new Framework(driver);

  const { number: CURRENT_BLOCK } = connex.thor.status.head;

  const loadExchangeData = async ([tokenAddress, exchangeAddress]) => {
    const token = new web3.eth.Contract(
      config.STR_ERC_20_ABI,
      tokenAddress,
    );

    const name = await token.methods.name().call();
    const symbol = await token.methods.symbol().call();
    const decimals = await token.methods.decimals().call();
    const balance = await token.methods.balanceOf(exchangeAddress).call(null, CURRENT_BLOCK);
    const vetBalance = await web3.eth.getBalance(exchangeAddress, CURRENT_BLOCK);
    const exchange = new web3.eth.Contract(config.EXCHANGE_ABI, exchangeAddress);
    const swapFee = await exchange.methods.swap_fee().call();
    const platformFee = await exchange.methods.platform_fee().call();

    return {
      balance,
      decimals,
      exchange,
      exchangeAddress,
      name,
      platformFee,
      swapFee,
      symbol,
      tokenAddress,
      vetBalance,
    }
  };

  const loadExchangeInfos = async () => {
    // get token count
    const tokenCount = await vexchangeFactory.methods.tokenCount().call();

    // get token addresses
    const loadTokens = _.times(tokenCount, (i) => {
      return vexchangeFactory.methods.getTokenWithId(i + 1).call()
    });
    const tokens = await Promise.all(loadTokens);

    // get exchange addresses
    const loadExchanges = tokens.map(token => {
      return vexchangeFactory.methods.getExchange(token).call();
    });
    const exchanges = await Promise.all(loadExchanges);

    const loadNewInfos = _.zip(tokens, exchanges).map(item => {
      return loadExchangeData(item);
    });

    const newInfos = await Promise.all(loadNewInfos);

    return newInfos;
  };

  const loadDailyVolume = async (infos) => {
    const from = moment().subtract(1, 'days').unix();
    const to = moment().unix();

    async.forEach(infos, async info => {
      const symbol = info.symbol.toLowerCase();

      let { data } = await axios.post(`http://45.32.212.120:8669/logs/event`, {
        "order": "desc",
        range: { unit: 'time', from, to },
        criteriaSet: [
          {
            address: info.exchangeAddress,
            topic0: config.EVENT_TOKEN_PURCHASE,
          },
          {
            address: info.exchangeAddress,
            topic0: config.EVENT_ETH_PURCHASE,
          },
        ]
      });

      let ethPurchaseABI = _.find(config.EXCHANGE_ABI, { name: 'EthPurchase' });
      let tokenPurchaseABI = _.find(config.EXCHANGE_ABI, { name: 'TokenPurchase' });

      let totalTradeVolume = new Proxy({}, handler);

      let tradeVolume = new Proxy({}, handler);
      let volume = [];
      info.volume = [];

      async.forEach(data, event => {
        switch(event.topics[0]) {
          case config.EVENT_TOKEN_PURCHASE:
            const tokenPurchaseEvent = new abi.Event(tokenPurchaseABI);
            const tokenPurchaseDecoded = tokenPurchaseEvent.decode(event.data, event.topics);

            let vetSold = ethers.BigNumber.from(tokenPurchaseDecoded.eth_sold);

            tradeVolume[tokenPurchaseDecoded.buyer] += parseInt(ethers.utils.formatEther(vetSold));
            totalTradeVolume[tokenPurchaseDecoded.buyer] += parseInt(ethers.utils.formatEther(vetSold));

            break;
          case config.EVENT_ETH_PURCHASE:
            const ethPurchaseEvent = new abi.Event(ethPurchaseABI);
            const ethPurchaseDecode = ethPurchaseEvent.decode(event.data, event.topics);

            let vetBought = ethers.BigNumber.from(ethPurchaseDecode.eth_bought);

            tradeVolume[ethPurchaseDecode.buyer] += parseInt(ethers.utils.formatEther(vetBought));
            totalTradeVolume[ethPurchaseDecode.buyer] += parseInt(ethers.utils.formatEther(vetBought));
            break;
          default:
            break;
        }
      });

      volume.push(tradeVolume);

      let totalVolume = _.sum(Object.values(totalTradeVolume));
      valuableTraders = filterObject(totalTradeVolume, vol => vol > totalVolume / 1000);
      valuableTraders = Object.keys(valuableTraders);

      info.totalVolume = totalVolume;
      info.valuableTraders = valuableTraders;

      async.forEach(volume, vol => {
        let filteredVol = new Proxy({}, handler);

        for (let trader in vol) {
          if (valuableTraders.includes(trader)) {
            filteredVol[trader] = vol[trader];
          } else {
            filteredVol.Other += vol[trader];
          }
        }

        info.volume.push(filteredVol);
      });

      fs.writeFileSync(`./data/volume/daily/${symbol}.json`, JSON.stringify({ totalVolume: info.totalVolume }));
    });
  };

  const populateLiquidityHistory = async infos => {
    for (const info of infos) {

      const symbol = info.symbol.toLowerCase();
      const account = connex.thor.account(info.exchangeAddress);
      const accountInfo = await account.get();

      const balanceOfABI = _.find(config.ERC_20_ABI, { name: 'balanceOf' });
      const balanceOf = connex.thor.account(info.tokenAddress).method(balanceOfABI);

      let tokenBalance = await balanceOf.call(info.exchangeAddress);
      tokenBalance = ethers.FixedNumber.fromValue(tokenBalance.decoded.balance, info.decimals);
      tokenBalance = parseInt(tokenBalance.toString());

      const balanceHex = ethers.BigNumber.from(accountInfo.balance);
      const balance = parseInt(ethers.utils.formatEther(balanceHex));

      const data = { balance, tokenBalance };
      fs.writeFileSync(`./data/liquidity/daily/${symbol}.json`, JSON.stringify( data ));
      console.log('saved liquidity');
    };
  };

  const main = async () => {
    let infos = [];

    try {

      infos = await loadExchangeInfos(infos);

      await loadDailyVolume(infos);
      await populateLiquidityHistory(infos);
    } catch(error) {
      console.log(error);
    }
  }

  main();
})();

