/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-13 10:09:12
 */

'use strict';

const BaseService = require('../base/service');

class DistrictService extends BaseService {

  /**
   * Get districts by country and province
   *
   * @param  {String}   countryCode Country code
   * @param  {String}   provinceCode Province code
   *
   * @param  {Function} result    Callback function
   */
  getManyByProvince(countryCode, provinceCode, result) {
    let self = this,
      opts = {};
    return self.responseMany(self.adapter.getManyByProvince(countryCode, provinceCode, opts), opts, result);
  }
}

module.exports = DistrictService;
