/*
 * @Author: toan.nguyen
 * @Date:   2016-08-07 23:43:16
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-12 16:27:47
 */

'use strict';

const helpers = require('node-helpers');
const CommonAdapter = require('../adapters/common');

class CommonService {

  /**
   * Adapter class for current service
   *
   * @return {Object} Adapter object
   */
  // get adapterClass() {
  //   return CommonAdapter;
  // }

  /**
   * Return exception handler
   *
   * @return {ExceptionHelper} Exception helpers
   */
  get exception() {
    return helpers.DefaultException;
  }

  /**
   * Constructor, set default data
   */
  constructor() {
    this.adapter = new this.adapterClass();
    this.model = new this.adapter.modelClass();
  }

  /**
   * Response result default data
   *
   * @param  {Promise} prom  Adapter promise function
   * @param  {Object} opts   Option data
   * @param  {Function} result Callback function
   */
  responseDefault(prom, opts, result) {
    let self = this;

    return prom.then(data => {
      console.log(data);
      return result(null, data);
    }, err => {
      return self.responseError(err, result);
    });
  }

  /**
   * Converts exeption from from postgres error
   *
   * @param  {Object} err    Postgres error
   * @param  {Function} result Callback result
   */
  responseError(err, result) {
    let ex = this.exception.create(err, {
      isPostgres: true,
      schema: this.adapter.tableSchema
    });

    return result(ex);
  }



  /**
   * Generates UID from database function
   *
   * @param  {Function} result Result callback
   */
  generateUid(result) {

    let opts = {};

    return this.responseDefault(this.adapter.generateUid(opts), opts, result);
  }
}

module.exports = CommonService;
