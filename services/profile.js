/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-14 17:39:01
 */

'use strict';

const BaseService = require('../base/service');

class ProfileService extends BaseService {

  /**
   * Get single object from database, filtered by username
   *
   * @param  {string} uesrname    Model username
   * @param  {Function} result Returned service data
   *
   */
  getByUsername(username, result) {

    let opts = {};

    return this.responseGetOne(this.adapter.getByUsername(username, opts), opts, result);
  }

  /**
   * Get single object from database, filtered by email
   *
   * @param  {string} email    Model email
   * @param  {Function} result Returned service data
   *
   */
  getByEmail(email, result) {

    let opts = {};

    return this.responseGetOne(this.adapter.getByEmail(email, opts), opts, result);
  }

  /**
   * Get single object from database, filtered by phone
   *
   * @param  {string} phone    Model phone number
   * @param  {Function} result Returned service data
   *
   */
  getByPhone(phone, result) {

    let opts = {};
    return this.responseGetOne(this.adapter.getByPhone(phone, opts), opts, result);
  }

  /**
   * Get single object from database, filtered by nexid
   *
   * @param  {string} nexid    Model NexID
   * @param  {Function} result Returned service data
   *
   */
  getOneByNexId(nexid, result) {

    let opts = {};
    return this.responseGetOne(this.adapter.getOneByNexId(nexid, opts), opts, result);
  }

  /**
   * Get single object from database, filtered by nexid, with relationship
   *
   * @param  {string} nexid    Model NexID
   * @param  {Function} result Returned service data
   *
   */
  getOneRelationByNexId(nexid, opts, result) {

    return this.responseGetOne(this.adapter.getOneRelationByNexId(nexid, opts), opts, result);
  }


  /**
   * Update avatar by user ID
   *
   * @param  {String}   uid       Primary key data
   * @param  {String}   avatar   Avatar json data
   *
   * @param  {Function} result Returned service data
   *
   */
  updateAvatar(uid, avatar, result) {
    let opts = {};
    return this.responseDefault(this.adapter.updateAvatar(uid, avatar, opts), opts, result);
  }

  /**
   * Update avatar by user ID
   *
   * @param  {String}   uid       Primary key data
   * @param  {String}   avatar   Avatar json data
   *
   * @param  {Function} result Returned service data
   *
   */
  updateVerificationCode(model, result) {
    let opts = {};
    return this.responseDefault(this.adapter.updateVerificationCode(model, opts), opts, result);
  }

  /**
   * Update avatar by user ID
   *
   * @param  {String}   uid       Primary key data
   * @param  {String}   avatar   Avatar json data
   *
   * @param  {Function} result Returned service data
   *
   */
  updateMetadata(uid, metadata, result) {
    let opts = {};
    return this.responseDefault(this.adapter.updateMetadata(uid, metadata, opts), opts, result);
  }

  /**
   * Updates notification token to user
   *
   * @param  {String} userId User ID
   * @param  {String} token  Notification token
   *
   * @return {Integer} Updated count
   */
  updateNotificationToken(userId, token, result) {
    let opts = {};
    return this.responseDefault(this.adapter.updateNotificationToken(userId, token), opts, result);
  }
}

module.exports = ProfileService;
