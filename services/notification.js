/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-10 16:12:33
 */

'use strict';

const BaseService = require('../base/service');

class NotificationService extends BaseService {


  /**
   * Update address from database, filtered by subject
   *
   * @param  {Object} model    Model data
   * @param  {Function} result Returned service data
   *
   */
  markAsRead(pks, result) {
    let opts = {};
    return this.responseDefault(this.adapter.markAsRead(pks, opts), opts, result);
  }

  /**
   * Update address from database, filtered by subject
   *
   * @param  {Object} model    Model data
   * @param  {Function} result Returned service data
   *
   */
  markAllAsRead(userId, type, result) {

    let opts = {};
    return this.responseDefault(this.adapter.markAllAsRead(userId, type, opts), opts, result);
  }

  /**
   * Count unread notifications of current user
   *
   * @param  {Object} model    Model data
   * @param  {Function} result Returned service data
   *
   */
  countUnread(userId, type, result) {

    let opts = {};
    return this.responseDefault(this.adapter.countUnread(userId, type, opts), opts, result);
  }

  /**
   * Virtual deletes one by user
   *
   * @param  {String}   userId   User ID
   * @param  {String}   type     Notification type
   * @param  {Object}   opts     Option data
   *
   * @return {mixed}   Number of deleted rows if successful. Error if failed
   */
  deleteOneByUser(pk, userId, result) {
    let opts = {};
    return this.responseDefault(this.adapter.deleteOneByUser(pk, userId, opts), opts, result);
  }
}

module.exports = NotificationService;
