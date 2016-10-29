/*
 * @Author: toan.nguyen
 * @Date:   2016-10-24 10:16:02
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-24 10:22:47
 */

'use strict';

const BaseService = require('../base/service');
const SystemSettingAdapter = require('../adapters/system-setting');

class SystemSettingService extends BaseService {

  /**
   * Adapter class for current service
   *
   * @return {Object} Adapter object
   */
  get adapterClass() {
    return SystemSettingAdapter;
  }

}

module.exports = SystemSettingService;
