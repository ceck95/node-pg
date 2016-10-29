/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-13 10:09:22
 */

'use strict';

const BaseService = require('../base/service');

class WardService extends BaseService {

  /**
   * Get wards by district, province and country
   *
   * @param  {String}   countryCode Country code
   * @param  {String}   provinceCode Province code
   * @param  {String}   districtCode District code
   *
   * @param  {Function} result    Callback function
   */
  getManyByDistrict(countryCode, provinceCode, districtCode, result) {
    let opts = {};
    return this.responseMany(this.adapter.getManyByDistrict(countryCode, provinceCode, districtCode, opts), opts, result);
  }
}

module.exports = WardService;
