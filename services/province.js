/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-13 10:09:18
 */

'use strict';

const BaseService = require('../base/service');

class ProvinceService extends BaseService {

  /**
   * Get provinces by country
   *
   * @param  {String}   countryCode Country code
   *
   * @param  {Function} result    Callback function
   */
  getManyByCountry(countryCode, result) {
    let opts = {};
    return this.responseMany(this.adapter.getManyByCountry(countryCode, opts), opts, result);
  }
}

module.exports = ProvinceService;
