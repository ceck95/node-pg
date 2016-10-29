/*
 * @Author: toan.nguyen
 * @Date:   2016-06-06 18:55:41
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-24 10:23:24
 */

'use strict';

module.exports = {
  pools: require('./base/pool').pools,
  helpers: require('./base/helpers'),
  adapters: {
    Base: require('./base/base-adapter'),
    Adapter: require('./base/pg-adapter'),
    Common: require('./adapters/common'),
    Profile: require('./adapters/profile'),
    Notification: require('./adapters/notification'),
    Address: require('./adapters/address'),
    Country: require('./adapters/country'),
    District: require('./adapters/district'),
    Province: require('./adapters/province'),
    Ward: require('./adapters/ward'),
    SystemSetting: require('./adapters/system-setting'),
  },
  services: {
    Base: require('./base/service'),
    Common: require('./services/common'),
    Profile: require('./services/profile'),
    Address: require('./services/address'),
    Notification: require('./services/notification'),
    Country: require('./services/country'),
    District: require('./services/district'),
    Province: require('./services/province'),
    Ward: require('./services/ward'),
    SystemSetting: require('./services/system-setting'),
  }
};
