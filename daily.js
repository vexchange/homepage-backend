require('dotenv').config();

const Web3 = require("web3");
const thorify = require("thorify").thorify;
const _ = require('lodash');
const async = require("async");
const moment = require('moment-timezone');
const fs = require('fs');
const CronJob = require('cron').CronJob;
const Framework = require('@vechain/connex-framework').Framework;
const ConnexDriver = require('@vechain/connex-driver');
const ethers = require('ethers').ethers;
const date = require('date-fns');

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

moment.tz.setDefault("America/New_York");

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

  const loadDailyVolume = async (infos, startBlock) => {
    async.forEach(infos, info => {
      if (info.symbol !== 'JUR') return;

      const symbol = info.symbol.toLowerCase();

      let ethPurchaseABI = _.find(config.EXCHANGE_ABI, { name: 'EthPurchase' });
      let ethPurchaseEvent = connex.thor.account(info.exchangeAddress).event(ethPurchaseABI);

      let tokenPurchaseABI = _.find(config.EXCHANGE_ABI, { name: 'TokenPurchase' });
      let tokenPurchaseEvent = connex.thor.account(info.exchangeAddress).event(tokenPurchaseABI);

      let ethPurchaseFilter = ethPurchaseEvent.filter([]);
      let tokenPurchaseFilter = tokenPurchaseEvent.filter([]);

      const daily = {
        unit: 'block',
        from: startBlock.block.number - 2,
        to: CURRENT_BLOCK,
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
          if (event.topics.includes(config.EVENT_ETH_PURCHASE)) {
            let vetBought = ethers.utils.bigNumberify(event.decoded.eth_bought);

            tradeVolume[event.decoded.buyer] += parseInt(ethers.utils.formatEther(vetBought));
            totalTradeVolume[event.decoded.buyer] += parseInt(ethers.utils.formatEther(vetBought));
          } else if (event.topics.includes(config.EVENT_TOKEN_PURCHASE)) {
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

  const getStartBlock = async () => {
    const target = moment().startOf('day').unix();

    let averageBlockTime = 17 * 1.5;

    let block = await connex.thor.block(CURRENT_BLOCK).get();

    let blockNumber = block.number;

    let requestsMade = 0;

    while (block.timestamp > target) {
      let decreaseBlocks = (block.timestamp - target) / averageBlockTime;
      decreaseBlocks = parseInt(decreaseBlocks);

      if (decreaseBlocks < 1) {
        break;
      }

      blockNumber -= decreaseBlocks;

      block = await connex.thor.block(blockNumber).get();
      requestsMade += 1
    }

    return { block , requestsMade };
  };

  const main = async () => {
    const startBlock = await getStartBlock();

    let infos = [];

    try {

      infos = await loadExchangeInfos(infos);

      await loadDailyVolume(infos, startBlock);
      await populateLiquidityHistory(infos);
    } catch(error) {
      console.log(error);
    }
  }

  //const job = new CronJob('*/30 * * * *', () => {
  //  main();
  //});

  main();
})();

