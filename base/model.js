/*
 * @Author: toan.nguyen
 * @Date:   2016-05-23 01:49:13
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-06-28 11:21:58
 */

'use strict';

const Hoek = require('hoek');
const config = require('config');
const moment = require('moment');

class BaseModel {

  /**
   * Return table name of model
   *
   * @return {String} table name
   */
  get tableName() {
    Hoek.assert(false, 'tableName has not been implemented');
  }

  /**
   * Return table schema name
   *
   * @return {String} Schema name
   */
  get tableSchema() {
    return config.get('db.postgres.default.schema');
  }

  /**
   * Return full table name of model
   *
   * @return {String} table name
   */
  get fullTableName() {
    return this.tableSchema + '.' + this.tableName;
  }

  /**
   * Return table alias
   *
   * @return {String} Table alias
   */
  get tableAlias() {
    Hoek.assert(false, 'tableAlias has not been implemented');
  }

  /**
   * Primary key name
   *
   * @return {string} Primary key name
   */
  get primaryKeyName() {
    return 'uid';
  }

  /**
   * Pre-processing data before insert and update
   */
  beforeSave(isNewRecord) {

    if (this.hasOwnProperty('metadata')) {
      if (this.metadata && typeof(this.metadata) !== 'string') {
        this.metadata = JSON.stringify(this.metadata);
      }
    }

    if (isNewRecord) {
      if (this.hasOwnProperty('createdAt')) {
        this.createdAt = moment.utc().format();
      } else if (this.hasOwnProperty('created')) {
        this.created = moment.utc().format();
      }
    }

    if (this.hasOwnProperty('updatedAt')) {
      this.updatedAt = moment.utc().format();
    } else if (this.hasOwnProperty('updated')) {
      this.updated = moment.utc().format();
    }
  }

  /**
   * Converts to thrift object
   *
   * @param {Object} opts Optional settings
   *
   * @return {sharedTypes.NexxAddress} Thrift address model
   */
  toThriftObject(opts) {
    throw 'toThriftObject method has not been implemented';
  }
}


module.exports = BaseModel;
