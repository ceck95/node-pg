/*
 * @Author: toan.nguyen
 * @Date:   2016-08-07 23:39:58
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-05 11:05:21
 */

'use strict';

const config = require('config');
const BPromise = require('bluebird');
const logger = require('../base/logger');
const BaseAdapter = require('../base/base-adapter');

class CommonAdapter extends BaseAdapter {

  /**
   * Constructor, set default data
   */
  constructor() {
    super();

    this.log = logger.child({
      namespace: 'postgres',
      adapter: 'CommonAdapter'
    });
  }

  /**
   * Returns table schema name
   *
   * @return {String}
   */
  get tableSchema() {
    return config.get('db.postgres.default.schema');
  }

  /**
   * Generates uid from database function
   *
   * @param  {Object}   opts     Option data
   *
   * @return {Promise}            Promise result
   */
  generateUid(opts) {

    let self = this,
      tableSchema = this.tableSchema,
      sql = 'SELECT ' + tableSchema + '.id_generator() as uid';

    return this.query(sql, [], opts).then(results => {
      self.log.debug('Generated UID:', results.rows[0].uid);
      return BPromise.resolve(results.rows[0].uid);
    });
  }
}

module.exports = CommonAdapter;
