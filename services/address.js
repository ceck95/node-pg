/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-12 10:02:29
 */

'use strict';

const BaseService = require('../base/service');

class AddressService extends BaseService {

  /**
   * Update address from database, filtered by subject
   *
   * @param  {Object} model    Model data
   * @param  {Function} result Returned service data
   *
   */
  updateBySubject(model, result) {

    var self = this,
      opts = {};

    return this.responseOne(self.adapter.updateBySubject(model, opts), opts, result);
  }

  /**
   * Get a record from subject
   *
   * @param  {String} subjectId Subject ID
   * @param  {Array} type       Subject type
   * @param  {Function} result Returned service data
   */
  getOneBySubject(subjectId, type, result) {

    let self = this,
      opts = {};

    return this.responseGetOne(self.adapter.getBySubject(subjectId, type, opts), opts, result);
  }

  /**
   * Get multiple records from subjects
   *
   * @param  {String} subjectIds Subject ID
   * @param  {Array} type       Subject type
   * @param  {Function} result Returned service data
   */
  getManyBySubject(subjectIds, type, result) {

    let self = this,
      opts = {};

    return this.responseMany(self.adapter.getBySubjects(subjectIds, type, opts), opts, result);
  }

  /**
   * Get or create new record by subject
   *
   * @param  {Object} model  Input data
   * @param  {Function} result Returned service data
   */
  getOrCreateBySubject(model, result) {

    let opts = {};

    return this.responseOne(this.adapter.getOrCreateBySubject(model, opts), opts, result);
  }
}

module.exports = AddressService;
