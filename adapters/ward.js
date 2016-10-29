/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 23:43:14
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-12 16:07:50
 */

'use strict';

const helpers = require('node-helpers');
const BaseAdapter = require('../base/pg-adapter');

const Ward = helpers.models.Ward;

class WardAdapter extends BaseAdapter {

  /**
   * Model class for current adapter
   *
   * @return {Class} Class object
   */
  get modelClass() {
    return Ward;
  }

  /**
   * Get wards by country, province and district
   *
   * @param  {String}   countryCode Country code
   * @param  {String}   provinceCode Province code
   * @param  {String}   districtCode District code
   * @param  {Object}   opts        Option data
   */
  getManyByDistrict(countryCode, provinceCode, districtCode, opts) {

    opts = opts || {};
    let self = this,
      model = new this.modelClass();

    return self.checkEmpty([{
      value: countryCode,
      message: 'Get ' + model.fullTableName + ' error: Input countryCode is null',
      source: 'countryCode'
    }, {
      value: provinceCode,
      message: 'Get ' + model.fullTableName + ' error: Input provinceCode is null',
      source: 'provinceCode'
    }, {
      value: districtCode,
      message: 'Get ' + model.fullTableName + ' error: Input districtCode is null',
      source: 'districtCode'
    }], opts).then(() => {
      let condition = {
        where: ['country_code = $1', 'province_code = $2', 'district_code = $3'],
        args: [countryCode, provinceCode, districtCode]
      };

      return this.getAllCondition(condition, opts);
    });
  }
}

module.exports = WardAdapter;
