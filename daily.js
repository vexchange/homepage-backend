require('dotenv').config();

const Web3 = require("web3");
const EthDater = require('ethereum-block-by-date');
const thorify = require("thorify").thorify;
const _ = require('lodash');
const async = require("async");
const moment = require('moment');
const fs = require('fs');
const schedule = require('node-schedule');
const Framework = require('@vechain/connex-framework').Framework;
const ConnexDriver = require('@vechain/connex-driver');
const ethers = require('ethers').ethers;

const NODE_URL = process.env.NODE_URL;

const web3 = thorify(new Web3(), NODE_URL);
const dater = new EthDater(web3);

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

  const { number: CURRENT_BLOCK } = await web3.eth.getBlock("latest");
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

  const loadDailyVolume = async (infos, blocks) => {
    async.forEach(infos, info => {
      const symbol = info.symbol.toLowerCase();

      let ethPurchaseABI = _.find(config.EXCHANGE_ABI, { name: 'EthPurchase' });
      let ethPurchaseEvent = connex.thor.account(info.exchangeAddress).event(ethPurchaseABI);

      let tokenPurchaseABI = _.find(config.EXCHANGE_ABI, { name: 'TokenPurchase' });
      let tokenPurchaseEvent = connex.thor.account(info.exchangeAddress).event(tokenPurchaseABI);

      let ethPurchaseFilter = ethPurchaseEvent.filter([]);
      let tokenPurchaseFilter = tokenPurchaseEvent.filter([]);

      const daily = {
        unit: 'block',
        from: _.first(blocks).block,
        to: _.last(blocks).block,
      };

      const limit = 256;

      Promise.all([
        ethPurchaseFilter.order('desc').range(daily).apply(0, limit),
        tokenPurchaseFilter.order('desc').range(daily).apply(0, limit),
      ]).then(([vetPurchaseEvents, tokenPurchaseEvents]) => {
        const events = [...vetPurchaseEvents, ...tokenPurchaseEvents];
        let totalTradeVolume = new Proxy({}, handler);

        let tradeVolume = new Proxy({}, handler);
        let volume = [];
        let info = {};
        info.volume = [];

        async.forEach(events, event => {
          if (event.topics[0] === config.EVENT_ETH_PURCHASE) {
            let vetBought = ethers.utils.bigNumberify(event.decoded.eth_bought);

            tradeVolume[event.decoded.buyer] += parseInt(ethers.utils.formatEther(vetBought)) / 0.997;
            totalTradeVolume[event.decoded.buyer] += parseInt(ethers.utils.formatEther(vetBought)) / 0.997;
          } else if (event.topics[0] === config.EVENT_TOKEN_PURCHASE) {
            let vetSold = ethers.utils.bigNumberify(event.decoded.eth_sold);

            tradeVolume[event.decoded.buyer] += parseInt(ethers.utils.formatEther(vetSold));
            totalTradeVolume[event.decoded.buyer] += parseInt(ethers.utils.formatEther(vetSold));
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

        fs.writeFileSync(`./data/volume/daily/${symbol}.json`, JSON.stringify(info));
        console.log('saved daily volume');
      }).catch(error => {
        console.log(error);
      });
    });
  };

  const populateLiquidityHistory = async infos => {
    async.forEach(infos, async info => {
      const symbol = info.symbol.toLowerCase();
      const account = connex.thor.account(info.exchangeAddress);
      const accountInfo = await account.get();

      const balanceOfABI = _.find(config.ERC_20_ABI, { name: 'balanceOf' });
      const balanceOf = connex.thor.account(info.tokenAddress).method(balanceOfABI);

      const tokenBalance = await balanceOf.call(info.exchangeAddress).then(data => {
        return parseInt(ethers.utils.formatEther(data.decoded.balance));
      }).catch(error => {
        console.log(error);
      });

      const balanceHex = ethers.utils.bigNumberify(accountInfo.balance)
      const balance = parseInt(ethers.utils.formatEther(balanceHex));

      const data = { balance, tokenBalance };
      fs.writeFileSync(`./data/liquidity/daily/${symbol}.json`, JSON.stringify( data ));
      //console.log('saved liquidity');
    });
  };

  const main = async () => {
    const start = moment().startOf('day');
    const now = moment();

    let infos = [];

    console.log('getting daily');
    try {
      let blocks = await dater.getEvery('minutes', start, now, 1, true);

      infos = await loadExchangeInfos(infos);

      await loadDailyVolume(infos, blocks);
      await populateLiquidityHistory(infos);
    } catch(error) {
      console.log(error);
    }
  }

  console.log('starting');
  main();

  schedule.scheduleJob('*/30 * * * *', () => {
    console.log('updating: ', moment().format());
    main();
  });
})();
