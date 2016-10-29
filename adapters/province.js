/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 23:43:14
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-12 16:08:01
 */

'use strict';

const helpers = require('node-helpers');
const BaseAdapter = require('../base/pg-adapter');

const Province = helpers.models.Province;

class ProvinceAdapter extends BaseAdapter {

  /**
   * Model class for current adapter
   *
   * @return {Class} Class object
   */
  get modelClass() {
    return Province;
  }


  /**
   * Get provinces by country
   *
   * @param  {String}   countryCode Country code
   * @param  {Object}   opts        Option data
   */
  getManyByCountry(countryCode, opts) {

    opts = opts || {};
    let self = this,
      model = new this.modelClass();

    return self.checkEmpty([{
      value: countryCode,
      message: 'Get ' + model.fullTableName + ' error: Input countryCode is null',
      source: 'countryCode'
    }], opts).then(() => {
      let condition = {
        where: ['country_code = $1'],
        args: [countryCode]
      };

      return this.getAllCondition(condition, opts);
    });
  }
}

module.exports = ProvinceAdapter;
