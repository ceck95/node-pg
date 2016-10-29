/*
 * @Author: toan.nguyen
 * @Date:   2016-06-28 09:02:08
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-08 13:36:49
 */

'use strict';

const Hoek = require('hoek');
const config = require('config');
const pgPool = require('pg-pool');
const BPromise = require('bluebird');
const logger = require('./logger');

let pgConfigs = config.get('db.postgres'),
  pools = {};

let connectPool = (listPools, key) => {

  return new BPromise((resolve, reject) => {
    let conn = Hoek.clone(pgConfigs[key].connection);
    conn.Promise = BPromise;
    if (pgConfigs[key].isDebug) {
      conn.log = logger.debug.bind(logger);
    }
    let pool = new pgPool(conn);
    // attach an error handler to the pool for when a connected, idle client
    // receives an error by being disconnected, etc
    pool.on('error', (error) => {
      console.log('Initializes postgres pool failed', error);
      return reject(error);
    });

    pool.configKey = key;
    listPools[key] = pool;

    console.log('Creates postgres pool ' + key + ' successfully');
    return resolve(pool);
  });

};

for (let key in pgConfigs) {
  connectPool(pools, key);
}

module.exports = {
  pools: pools,
  connectPool: connectPool
};
