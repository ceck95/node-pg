/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 23:43:14
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-12 16:08:09
 */

'use strict';

const helpers = require('node-helpers');
const BaseAdapter = require('../base/pg-adapter');

const District = helpers.models.District;

class DistrictAdapter extends BaseAdapter {


  /**
   * Model class for current adapter
   *
   * @return {Class} Class object
   */
  get modelClass() {
    return District;
  }

  /**
   * Get districts by country and province
   *
   * @param  {String}   countryCode Country code
   * @param  {String}   provinceCode Province code
   * @param  {Object}   opts        Option data
   */
  getManyByProvince(countryCode, provinceCode, opts) {

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
    }], opts).then(() => {
      let condition = {
        where: ['country_code = $1', 'province_code = $2'],
        args: [countryCode, provinceCode]
      };

      return this.getAllCondition(condition, opts);
    });

  }
}

module.exports = DistrictAdapter;
