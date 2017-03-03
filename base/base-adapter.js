/**
 * @Author: Tran Van Nhut <nhutdev>
 * @Date:   2016-10-29T22:20:09+07:00
 * @Email:  tranvannhut4495@gmail.com
* @Last modified by:   nhutdev
* @Last modified time: 2017-03-03T17:55:50+07:00
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
