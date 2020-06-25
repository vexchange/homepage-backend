const thorify = require("thorify").thorify;
const EthDater = require('ethereum-block-by-date');
const _ = require('lodash');
const Web3 = require("web3");
const ethers = require('ethers').ethers;
const async = require("async");
const save = require('./save')
const moment = require('moment');
const fs = require('fs');
const argv = require('minimist')(process.argv.slice(2));
const Framework = require('@vechain/connex-framework').Framework;
const ConnexDriver = require('@vechain/connex-driver');

const config = require('./config');

const web3 = thorify(new Web3(), 'http://45.32.212.120:8669/');
const dater = new EthDater(web3);

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
  const { Driver, SimpleNet, SimpleWallet, options } = ConnexDriver;

  const driver = await Driver.connect(new SimpleNet('http://45.32.212.120:8669/'));
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

  const wait = ms => {
    return new Promise(resolve => setTimeout(resolve, ms));
  };

  const getLogs = async (addresses, topics, startBlock) => {
    const getChunks = (start, address) => {
      return web3.eth.getPastLogs({
        fromBlock: start,
        toBlock: CURRENT_BLOCK,
        address: address,
        topics,
      })
    };

    const logs = addresses.map(async address => {
      return await getChunks(startBlock, address);
    });

    // these may be costly
    // look at performance
    return Promise.all(logs)
      .then(data => data.filter(item => item.length))
      .then(data => _.flatten(data))
      .then(data => _.groupBy(data, 'address'));
  };
  
  const loadLogs = async (startBlock, infos) => {
    const prefix = '0x000000000000000000000000';
    const exchangeAddresses = infos.map(info => info.exchangeAddress);
    const tokenAddresses = infos.map(info => info.tokenAddress);
    const exchangeLogs = await getLogs(exchangeAddresses, [config.ALL_EVENTS], startBlock);
    const exchangeTopics = exchangeAddresses.map(address => prefix + address.substring(2))
    const tokenLogs = await getLogs(tokenAddresses, [[config.EVENT_TRANSFER], [], exchangeTopics], startBlock)
    let logs1 = [];
    let logs2 = [];

    async.forEach(infos, info => {
      const newExchangeLogs = exchangeLogs[info.exchangeAddress.toLowerCase()];
      const newTokenLogs = tokenLogs[info.tokenAddress.toLowerCase()];

      if (newExchangeLogs?.length) {
        logs1.push(newExchangeLogs);
      }

      if (newTokenLogs?.length) {
        logs2.push(newTokenLogs);
      }

      return _.flatten([...logs1, ...logs2]);
    }, () => {

    });
  };

  const populateProviders = infos => {
    return new Promise(resolve => {
      async.forEach(infos, async info => {
        const exchange = new web3.eth.Contract(config.EXCHANGE_ABI, info.exchangeAddress);

        const events = await exchange.getPastEvents('Transfer');

        info.providers = new Proxy({}, handler);

        async.forEach(events, event => {
          if (event.raw.topics[0] != config.EVENT_TRANSFER || event.address == info.exchangeAddress) {
            return;
          }

          if (event.returnValues['_from'] == ethers.constants.AddressZero) {
            let ether = ethers.utils.bigNumberify(event.returnValues['_value']);

            info.providers[event.returnValues['_to']] += parseInt(ethers.utils.formatEther(ether), 10)
          } else if (event.returnValues['_to'] == ethers.constants.AddressZero) {
            ether = ethers.utils.bigNumberify(event.returnValues['_value']);

            info.providers[event.returnValues['_from']] -= parseInt(ethers.utils.formatEther(ether), 10);
          } else {
            ether = ethers.utils.bigNumberify(event.returnValues['_value']);

            info.providers[event.returnValues['_from']] -= parseInt(ethers.utils.formatEther(ether), 10);
            info.providers[event.returnValues['_to']] += parseInt(ethers.utils.formatEther(ether), 10);
          }

        });
      }, () => {
        resolve(infos);
      });
    });
  };

  const populateRoi = (infos) => {
    console.log('hit');
    let vetBalance = 0;
    let tokenBalance = 0;

    return new Promise((resolve) => {

      async.forEach(infos, async info => {
        const exchange = new web3.eth.Contract(config.EXCHANGE_ABI, info.exchangeAddress);
        const events = await exchange.getPastEvents('allEvents');

        info.roi = [];
        info.history = [];

        async.forEach(events, event => {
          let dmNumerator = 1;
          let dmDenominator = 1;
          let tradeVolume = 0;

          if (event.raw.topics[0] == config.EVENT_TRANSFER) {
            if (event.address == info.exchangeAddress) {
              return;
            } else {
              if (event.event == 'Transfer') {
                if (event.returnValues['_to'] != info.exchangeAddress) {
                  return;
                }
              }

              if (tokenBalance > 0) {
                let value = ethers.utils.bigNumberify(event.returnValues._value);
                dmNumerator *= tokenBalance + parseInt(ethers.utils.formatEther(value), 10);
                dmDenominator *= tokenBalance;
              }

              let value = ethers.utils.bigNumberify(event.returnValues._value);

              tokenBalance += parseInt(ethers.utils.bigNumberify(value));
            }
          } else if (event.raw.topics[0] == config.EVENT_ADD_LIQUIDITY) {
            let vBalance = ethers.utils.bigNumberify(event.returnValues['eth_amount']);
            let tBalance = ethers.utils.bigNumberify(event.returnValues['token_amount']);

            vetBalance += parseInt(ethers.utils.formatEther(vBalance), 10);
            tokenBalance += parseInt(ethers.utils.formatEther(tBalance), 10);
          } else if (event.raw.topics[0] == config.EVENT_REMOVE_LIQUIDITY) {
            let vBalance = ethers.utils.bigNumberify(event.returnValues['eth_amount']);
            let tBalance = ethers.utils.bigNumberify(event.returnValues['token_amount']);

            vetBalance -= parseInt(ethers.utils.formatEther(vBalance), 10);
            tokenBalance -= parseInt(ethers.utils.formatEther(tBalance), 10);
          } else if (event.raw.topics[0] == config.EVENT_ETH_PURCHASE) {
            let vetBought = ethers.utils.bigNumberify(event.returnValues.eth_bought);
            let tokensSold = ethers.utils.bigNumberify(event.returnValues.tokens_sold);

            let vetNewBalance = vetBalance + parseInt(ethers.utils.formatEther(vetBought), 10);
            let tokenNewBalance = tokenBalance + parseInt(ethers.utils.formatEther(tokensSold), 10);

            dmNumerator *= vetNewBalance * tokenNewBalance;
            dmDenominator *= vetBalance * tokenBalance;

            tradeVolume += parseInt(ethers.utils.formatEther(vetBought), 10) / 0.997;
            vetBought = vetNewBalance;
            tokenBalance = tokenNewBalance;
          } else {
            if (event.raw.topics[0] == config.EVENT_TOKEN_PURCHASE) {
              let vetSold = ethers.utils.bigNumberify(event.returnValues.eth_sold);
              let tokensBought = ethers.utils.bigNumberify(event.returnValues.tokens_bought);

              let vetNewBalance = vetBalance + parseInt(ethers.utils.formatEther(vetSold), 10);
              let tokenNewBalance = tokenBalance + parseInt(ethers.utils.formatEther(tokensBought), 10);

              dmNumerator *= vetNewBalance * tokenNewBalance;
              dmDenominator *= vetBalance * tokenBalance;

              tradeVolume += parseInt(ethers.utils.formatEther(vetSold), 10);
              vetBought = vetNewBalance;
              tokenBalance = tokenNewBalance;
            }
          }

          info.roi.push({
            dmChange: Math.sqrt(dmNumerator / dmDenominator),
            vetBalance,
            tokenBalance,
            tradeVolume,
          })
          info.history.push(vetBalance);
        });
      }, () => {
        resolve(infos);
      });
    })
  };

  const populateVolume = infos => {
    return new Promise((resolve) => {
      async.forEach(infos, async info => {
        const exchange = new web3.eth.Contract(config.EXCHANGE_ABI, info.exchangeAddress);
        const events = await exchange.getPastEvents('allEvents');

        let totalTradeVolume = new Proxy({}, handler);

        let tradeVolume = new Proxy({}, handler);
        let volume = [];
        info.volume = [];

        async.forEach(events, event => {
          if (event.raw.topics[0] == config.EVENT_ETH_PURCHASE) {
            let vetBought = ethers.utils.bigNumberify(event.returnValues.eth_bought);

            tradeVolume[event.returnValues.buyer] += parseInt(ethers.utils.formatEther(vetBought)) / 0.997;
            totalTradeVolume[event.returnValues.buyer] += parseInt(ethers.utils.formatEther(vetBought)) / 0.997;
          } else if (event.raw.topics[0] == config.EVENT_TOKEN_PURCHASE) {
            let vetSold = ethers.utils.bigNumberify(event.returnValues.eth_sold);

            tradeVolume[event.returnValues.buyer] += parseInt(ethers.utils.formatEther(vetSold));
            totalTradeVolume[event.returnValues.buyer] += parseInt(ethers.utils.formatEther(vetSold));
          }

        });

        volume.push(tradeVolume);

        let totalVolume = _.sum(Object.values(totalTradeVolume));
        valuableTraders = filterObject(totalTradeVolume, vol => vol > totalVolume / 1000);
        valuableTraders = Object.keys(valuableTraders);

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
      }, () => {
        resolve(infos);
      });
    });
  };

  const isValuable = info => {
    return info.vetBalance >= 200 * config.VET;
  };

  const isEmpty = info => {
    return info.vetBalance <= config.VET
  };

  const getChartRange = (start = config.HISTORY_BEGIN_BLOCK) => {
    return _.range(start, CURRENT_BLOCK, config.HISTORY_CHUNK_SIZE);
  };

  const loadTimestamps = async () => {
    const range = getChartRange();
    const timestamps = [];
    let i = 0;
    return new Promise((resolve) => {
      async.forEach(range, async () => {
        const { timestamp } = await web3.eth.getBlock(i);
        timestamps.push(timestamp);
        await wait(500);
        i++
      }, () => {
        resolve(timestamps);
      });
    });
  };

  const saveLiquidityData = async (infos, range) => {
    const valuableInfos = infos.filter(info => isValuable(info));
    const otherInfos = infos.filter(info => !isValuable(info));

    const getLiquidity = async.map(valuableInfos, async info => {
      let jerry = {
        [info.symbol]: [],
      };

      for (let j = 0; j <= range.length; j++) {
        if (range[j]) {
          const { timestamp } = await web3.eth.getBlock(range[j]);

          jerry[info.symbol].push({
            timestamp: timestamp * 1000,
            history: info.history[j]
          });
        }
      }

      return jerry;
    });

    const data = await getLiquidity;

    fs.writeFileSync('./data/liquidity.json', JSON.stringify(data));
  };

  const saveTotalVolumeData = async (infos, range) => {
    const valuableInfos = infos.filter(info => isValuable(info));
    const otherInfos = infos.filter(info => !isValuable(info));

    const getVolume = async.map(valuableInfos, async info => {
      let jerry = {
        [info.symbol]: [],
      };

      for (let j = 0; j <= range.length; j++) {
        if (range[j]) {
          const { timestamp } = await web3.eth.getBlock(range[j]);

          jerry[info.symbol].push({
            timestamp: timestamp * 1000,
            volume: _.sum(Object.values(info.volume[j])),
          });
        }
      }

      return jerry;
    });

    const data = await getVolume;

    fs.writeFileSync('./data/volume.json', JSON.stringify(data));
  };

  const saveProvidersData = async infos => {
    async.forEach(infos, info => {
      const totalSupply = _.sum(Object.values(info.providers));
      const providers = Object.entries(info.providers);
      const symbol = info.symbol.toLowerCase();
      let remainingSuplly = totalSupply;
      let data = [];

      providers.forEach(provider => {
        const [address, value] = provider;
        const s = value / totalSupply;
        if (s >= 0.01) {
          data.push({
            address,
            balance: info.vetBalance * s / config.VET
          });

          fs.writeFileSync(`./data/tokens/${symbol}.json`, JSON.stringify(data));

          remainingSuplly -= value;
        }
      });
    });
  };

  const saveRoiData = async (infos, range) => {
    async.forEach(infos, async info => {
      const symbol = info.symbol.toLowerCase();
      let data = [];

      for (let j = 0; j <= range.length; j++) {
        if (range[j]) {
          const { timestamp } = await web3.eth.getBlock(range[j]);

          data.push({
            timestamp,
            roi: info.roi[j].dmChange,
            price: info.roi[j].tokenBalance / info.roi[j].vetBalance,
            volume: info.roi[j].tradeVolume 
          });
        }
      }

      fs.writeFileSync(`./data/roi/${symbol}.json`, JSON.stringify(data));
    });
  };

  const saveVolumeData = async (infos, range) => {
    async.forEach(infos, async info => {
      const symbol = info.symbol.toLowerCase();
      let data = [];

      const getVolume = async.map(info.valuableTraders, async trader => {
        for (let j = 0; j <= range.length; j++) {
          if (range[j]) {
            const { timestamp } = await web3.eth.getBlock(range[j].block);

            if (!info.volume[j]) continue;

            data.push({
              block: range[j].block,
              timestamp: timestamp * 1000,
              volume: info.volume[j][trader]
            });
          }
        }
      });

      await getVolume;

      fs.writeFileSync(`./data/volume/${symbol}.json`, JSON.stringify(data));
    });
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
      });

      const balanceHex = ethers.utils.bigNumberify(accountInfo.balance)
      const balance = parseInt(ethers.utils.formatEther(balanceHex));

      const data = { balance, tokenBalance };
      fs.writeFileSync(`./data/liquidity/daily/${symbol}.json`, JSON.stringify( data ));
      console.log('saved liquidity');
    });
  };

  const main = async () => {
    let infos = [];

    infos = await loadExchangeInfos(infos);

    if (argv.daily) {
      console.log('getting daily');
      let blocks = await dater.getEvery('minutes', moment().subtract(1, 'days'), moment(), 1, true);

      await loadDailyVolume(infos, blocks);
      await populateLiquidityHistory(infos);
    }

    //logs = await loadLogs(config.GENSIS_BLOCK_NUMBER, infos);
    //infos = await populateProviders(infos);
    //infos = await populateRoi(infos);
    //infos = await populateVolume(infos);

    //const range = await getBlocksBy();

    //const range = getChartRange();
    //const notEmptyInfos = infos.filter(info => !isEmpty(info));

    //save.lastBlock(CURRENT_BLOCK);
    //saveLiquidityData(infos, range);
    //saveTotalVolumeData(infos, range);
    //saveProvidersData(notEmptyInfos);
    //saveRoiData(notEmptyInfos, range);
    //saveVolumeData(notEmptyInfos, range);
  }

  main();
})();

