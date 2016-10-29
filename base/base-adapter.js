/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 21:38:29
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-10 18:41:03
 */

'use strict';

const pool = require('./pool');
const helpers = require('node-helpers');
const BPromise = require('bluebird');

class BaseAdapter {

  /**
   * Default Log namespace
   *
   * @return {String}
   */
  get logNamespace() {
    return 'postgres';
  }

  /**
   * Returns postgres query timeout in milliseconds
   *
   * @return {Integer}
   */
  get queryTimeout() {
    return 30000;
  }

  /**
   * Returns postgres connection pool
   *
   * @return {Pool} Postgres connection pool
   */
  get pool() {
    if (this.connectionName) {
      return pool.pools[this.connectionName];
    }

    return pool.pools.default;
  }

  /**
   * Catch exception Error callback service
   * @param  {Object} e     Error data
   */
  catchException(e) {

    if (helpers.isDebug) {
      throw e;
    }

    this.log.error('Query error', e.message, e.stack);
  }

  /**
   * Query SQL, args value
   *
   * @param  {String} sql  SQL string
   * @param  {Array} args  SQL arguments
   * @param {Object} opts Option data
   *
   * @return {Bluebird}
   */
  query(sql, args) {
    let self = this;

    return new BPromise((resolve, reject) => {

      let reconnectFunc = () => {
        // release all client, and reconnect
        return queryFunc();
      };

      let queryFunc = () => {
        self.log.debug('SQL:', sql, ' Args:', args);

        return self.pool.query(sql, args).then((result) => {
          self.log.debug('Query successfully. Count:', result.rowCount);
          return resolve(result);
        }).catch(e => {
          console.error(e);
          if (e.code === 'EAI_AGAIN') {
            return reconnectFunc();
          }
          // releaseFunc();
          self.log.error('Query Error:', e, 'SQL:', sql, 'Args:', args);
          // self.catchException(e);
          return reject(e);
        });
      };

      return queryFunc();
    });
  }

  /**
   * Check empty input data
   *
   * @param  {mixed}   input    Input data
   * @param  {Object}   opts    Option data
   *
   * @return {Boolean}           Is input empty
   */
  checkEmpty(inputs, opts) {
    opts = opts || {};
    let self = this;
    opts.log = this.log;

    return new BPromise((resolve, reject) => {
      return helpers.Error.checkEmpty(inputs, opts).then(() => {
        return resolve();
      }, err => {
        if (opts.client) {
          opts.client.release();
        }

        return reject(err);
      }).catch(e => {
        return self.catchException(e);
      });
    });
  }


}

module.exports = BaseAdapter;
