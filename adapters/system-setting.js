/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 23:43:14
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-24 10:15:04
 */

'use strict';

const helpers = require('node-helpers');
const BaseAdapter = require('../base/pg-adapter');

const SystemSetting = helpers.models.SystemSetting;

class SystemSettingAdapter extends BaseAdapter {

  /**
   * Model class for current adapter
   *
   * @return {Class} Class object
   */
  get modelClass() {
    return SystemSetting;
  }
}

module.exports = SystemSettingAdapter;
