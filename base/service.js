/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last modified by:   nhutdev
 * @Last modified time: 2017-03-23T14:27:33+07:00
 */

'use strict';

const Hoek = require('hoek');
const helpers = require('node-helpers');
const PaginationModel = helpers.models.PaginationModel;

class BaseService {

  /**
   * Adapter class for current service
   *
   * @return {Object} Adapter object
   */
  get adapterClass() {
    Hoek.assert(false, 'adapterClass has not been implemented');
  }

  /**
   * Thrift class model
   *
   * @return {Object} Thrift class object
   */
  get paginationThriftClass() {
    Hoek.assert(false, 'paginationThriftClass has not been implemented');
  }

  /**
   * Return exception handler
   *
   * @return {ExceptionHelper} Exception helpers
   */
  get exception() {
    return helpers.DefaultException;
  }

  /**
   * Constructor, set default data
   *
   */
  constructor() {
    this.adapter = new this.adapterClass();
    this.model = new this.adapter.modelClass();
  }


  /**
   * Response result default data
   *
   * @param  {Promise} prom  Adapter promise function
   * @param  {Object} opts   Option data
   * @param  {Function} result Callback function
   */
  responseDefault(prom, opts, result) {
    let self = this;

    return prom.then(data => {
      return result(null, data);
    }, err => {
      return self.responseError(err, result);
    });
  }

  /**
   * Creates model thrift
   *
   * @param  {Object} data Input data
   * @param  {Object} opts Option data
   *
   * @return {Model}      Model thrift
   */
  createThriftModel(data, opts) {

    if (!data) {
      return null;
    }
    if (data.toThriftObject) {
      return data.toThriftObject();
    }

    if (opts.thriftClass) {
      let model = new opts.thriftClass();
      helpers.Model.assignData(model, data, opts);
      return model;
    }

    let modelClass = opts.modelClass || this.adapter.modelClass,
      resultModel = new modelClass(data, opts);

    if (resultModel.toThriftObject) {
      return resultModel.toThriftObject();
    }

    helpers.models.assignData(resultModel, data);
    return resultModel;
  }

  /**
   * Response result with a instance model
   *
   * @param  {Object} err  Error response from adapter
   * @param  {Object} data Data response from adapter
   *
   * @return {Function}      Callback result
   */
  responseOne(prom, opts, result) {
    opts = opts || {};
    let self = this;

    return prom.then(data => {
      let resultModel = self.createThriftModel(data, opts);
      return result(null, resultModel);
    }, err => {
      self.adapter.log.error(err);
      return self.responseError(err, result);
    });
  }

  /**
   * Converts exeption from from postgres error
   *
   * @param  {Object} err    Postgres error
   * @param  {Function} result Callback result
   */
  responseError(err, result) {
    let ex = this.exception.create(err, {
      isPostgres: true,
      schema: this.model.tableSchema,
      table: this.model.tableName
    });

    return result(ex);
  }

  /**
   * Response result with a instance model
   *
   * @param  {Object} err  Error response from adapter
   * @param  {Object} data Data response from adapter
   *
   * @return {Function}      Callback result
   */
  responseGetOne(prom, opts, result) {
    let self = this;

    return prom.then(data => {
      if (!data) {
        return self.responseError(helpers.Error.notFound(), result);
      }
      let resultModel = self.createThriftModel(data, opts);
      return result(null, resultModel);
    }, err => {
      self.adapter.log.error(err);
      return self.responseError(err, result);
    });
  }


  /**
   * Response result with a instance model
   *
   * @param  {Object} err  Error response from adapter
   * @param  {Object} data Data response from adapter
   *
   * @return {Function}      Callback result
   */
  responseGetRelation(prom, opts, result) {
    let self = this;

    return prom.then(data => {
      if (!data) {
        return self.responseError(helpers.Error.notFound(), result);
      }

      let resultModel = self.createThriftModel(data, opts);
      return result(null, resultModel);
    }, err => {
      self.adapter.log.error(err);
      return self.responseError(err, result);
    });
  }

  /**
   * Response result with multiple model instances
   *
   * @param  {Object} err  Error response from adapter
   * @param  {Object} data Data response from adapter
   *
   * @return {Function}      Callback result
   */
  responseMany(prom, opts, result) {
    let self = this;

    return prom.then(data => {

      let responseData = [];
      if (!data) {
        return result(null, responseData);
      }

      data.forEach(e => {
        let m = self.createThriftModel(e, opts);
        responseData.push(m);
      });

      return result(null, responseData);
    }, err => {
      self.adapter.log.error(err);
      return self.responseError(err, result);
    });
  }

  /**
   * Response result with multiple model instances, with pagination
   *
   * @param  {Object} err  Error response from adapter
   * @param  {Object} data Data response from adapter
   *
   * @return {Function}      Callback result
   */
  responsePagination(prom, pagingParams, result) {
    let self = this;

    return prom.then(resp => {

      let response = new self.model.paginationThriftClass(),
        pagination = new PaginationModel(resp.meta),
        responseData = [];

      response.pagination = pagination.toThriftObject();

      resp.data.forEach((element) => {
        let m = self.createThriftModel(element, pagingParams);

        responseData.push(m);
      });

      response.data = responseData;
      return result(null, response);
    }, err => {
      self.adapter.log.error(err);
      return self.responseError(err, result);
    });
  }

  /**
   * Insert model into database
   *
   * @param  {Object} form  Form data
   * @param  {Function} result Result callback
   */
  insertOne(form, result) {

    let opts = {};

    return this.responseOne(this.adapter.insertOne(form, opts), opts, result);
  }

  /**
   * Insert many models into database
   *
   * @param  {Array} models  Models data
   * @param  {Function} result Result callback
   */
  insertMany(models, result) {
    let opts = {};

    return this.responseMany(this.adapter.insertMany(models), opts, result);
  }


  /**
   * Update model into database
   *
   * @param  {Object} form  Form data
   * @param  {Function} result Result callback
   */
  updateOne(form, opts, result) {

    opts = opts || {};

    if (typeof(opts) === 'function') {
      result = opts;
      opts = {};
    }

    return this.responseOne(this.adapter.updateOne(form, opts), opts, result);
  }

  /**
   * Get single object from database, return service
   *
   * @param  {mixed} condition     query condition
   * @param  {Function} result Returned service data
   */
  getOne(condition, result) {

    let opts = {};
    return this.responseGetOne(this.adapter.getOne(condition, opts), opts, result);
  }

  /**
   * Get single object from database, return service
   *
   * @param  {mixed} condition     query condition
   * @param  {Function} result Returned service data
   */
  getOneByPk(pk, result) {

    let opts = {};

    return this.responseGetOne(this.adapter.getOneByPk(pk, opts), opts, result);
  }

  /**
   * Get single object from database, return service
   *
   * @param  {mixed} condition     query condition
   * @param  {Function} result Returned service data
   */
  getOneRelation(condition, opts, result) {

    opts = opts || {};
    if (Array.isArray(opts)) {
      opts = {
        includes: opts
      };
    }

    return this.responseGetOne(this.adapter.getOneRelation(condition, opts), opts, result);
  }

  /**
   * Get single object from database, return service
   *
   * @param  {mixed} condition     query condition
   * @param  {Function} result Returned service data
   */
  getOneRelationByPk(pk, opts, result) {
    return this.responseGetOne(this.adapter.getOneRelationByPk(pk, opts), opts, result);
  }

  /**
   * Check exists by condition
   *
   * @param  {mixed} uid    Unique Primary key value
   * @param  {Function} result Callback result
   */
  exists(condition, result) {

    let opts = {};

    return this.responseDefault(this.exists, opts, result);
  }

  /**
   * Get multiple objects from database, return service
   *
   * @param  {Array} pks     Primary key
   * @param {Object} opts Optional data
   * @param  {Function} result Returned service data
   */
  getMany(pks, result) {

    let opts = {};
    return this.responseMany(this.adapter.getMany(pks, opts), opts, result);
  }

  /**
   * Get multiple objects from database with relationship, return service
   *
   * @param  {Array} pks     Primary key
   * @param {Object} opts Optional data
   *
   * @param  {Function} result Returned service data
   */
  getManyRelation(pks, opts, result) {

    opts = opts || {};
    if (Array.isArray(opts)) {
      opts = {
        includes: opts
      };
    }

    return this.responseMany(this.adapter.getManyRelation(pks, opts), opts, result);
  }

  /**
   * Get all records from database, return service
   *
   * @param {Object} opts Optional data
   * @param  {Function} result Returned service data
   */
  getAll(result) {

    let opts = {};

    return this.responseMany(this.adapter.getAll(opts), opts, result);
  }

  /**
   * Query all rows from database, without relations
   * Filtered by status
   *
   * @param  {Integer} status Status
   * @param  {Function} result Callback function
   *
   * @return {object}   Result model
   */
  getAllStatus(status, result) {

    let opts = {};
    return this.responseMany(this.adapter.getAllStatus(status), opts, result);
  }

  /**
   * Query all active rows from database, without relations
   *
   * @param  {Function} result Callback function
   *
   * @return {object}   Result model
   */
  getAllActive(result) {
    let opts = {};

    return this.responseMany(this.adapter.getAllActive(opts), opts, result);
  }

  /**
   * Query all inactive rows from database, without relations, with order
   *
   * @param  {Function} result Callback function
   *
   * @return {object}   Result model
   */
  getAllInactive(result) {
    let opts = {};

    return this.responseMany(this.adapter.getAllInactive(opts), opts, result);
  }

  /**
   * Query all disabled rows from database, without relations, with order
   *
   * @param  {Function} result Callback function
   *
   * @return {object}   Result model
   */
  getAllDisabled(result) {
    let opts = {};

    return this.responseMany(this.adapter.getAllDisabled(opts), opts, result);
  }

  /**
   * Query all deleted rows from database, without relations, with order
   *
   * @param  {Function} result Callback function
   *
   * @return {object}   Result model
   */
  getAllDeleted(result) {
    let opts = {};

    return this.responseMany(this.adapter.getAllDeleted(opts), opts, result);
  }


  /**
   * Get all records from database, with order return service
   *
   * @param {String} order Order SQL string
   * @param {Object} opts Optional data
   * @param  {Function} result Returned service data
   */
  getAllOrder(order, result) {
    let opts = {};

    return this.responseMany(this.adapter.getAllOrder(order, opts), opts, result);
  }

  /**
   * Get all records from database, with relationship and pagination, return service
   *
   * @param  {Object} pagingParams     Pagination params
   * @param  {Function} result Returned service data
   */
  getPagination(pagingParams, result) {

    var self = this,
      opts = {
        prefix: this.model.tableAlias
      };

    if (self.adapter.sqlRelation) {
      pagingParams.prefix = this.model.tableAlias;
    }

    return this.responsePagination(this.adapter.getPagination(pagingParams, opts), pagingParams, result);
  }

  /**
   * Get multiple rows from table, with condition
   *
   * @param  {object} params Query params
   * @param  {Object} opts Optional data
   * @param  {Function} result Callback function
   *
   */
  getAllCondition(params, opts, result) {
    return this.responseMany(this.adapter.getAllCondition(params, opts), opts, result);
  }

  getAllConditionRelation(condition, opts, result) {
    return this.responseMany(this.adapter.getAllConditionRelation(condition, opts), opts, result);
  }


  /**
   * Filter multiple objects from database, return service
   *
   * @param  {Object} params     Parameters filter data
   * @param  {Object} pagingParams     Pagination params
   * @param  {Function} result Returned service data
   */
  filter(params, pagingParams, result) {

    var self = this,
      opts = {
        prefix: this.model.tableAlias
      };

    if (self.adapter.sqlRelation) {
      pagingParams.prefix = this.model.tableAlias;
    }

    return this.responseMany(this.adapter.filter(params, pagingParams, opts), pagingParams, result);
  }

  /**
   * Filter multiple objects from database, return service
   *
   * @param  {Object} params     Parameters filter data
   * @param  {Object} pagingParams     Pagination params
   * @param  {Function} result Returned service data
   */
  filterPagination(params, pagingParams, result) {

    var self = this,
      opts = {
        prefix: this.model.tableAlias
      };

    if (self.adapter.sqlRelation) {
      pagingParams.prefix = this.model.tableAlias;
    }

    return this.responsePagination(this.adapter.filterPagination(params, pagingParams, opts), pagingParams, result);
  }

  /**
   * Delete many record by condition
   *
   * @param  {Object} condition    Query condition
   * @param  {Function} result Callback result
   */
  deleteMany(condition, result) {

    let opts = {};

    return this.responseDefault(this.adapter.deleteMany(condition, opts), opts, result);
  }

  /**
   * Delete one record by primary key
   *
   * @param  {mixed} pk    Unique Primary key value
   * @param  {Function} result Callback result
   */
  deleteByPk(pk, result) {

    let opts = {};

    return this.responseDefault(this.adapter.deleteByPk(pk, opts), opts, result);
  }

  /**
   * Get or create new record, return retrieved record data
   *
   * @param  {Object} model  Model data
   * @param  {Function} result Returned service data
   */
  getOrCreate(model, result) {

    let self = this,
      opts = {};

    return this.responseOne(self.adapter.getOrCreate(model, opts), opts, result);
  }
}

module.exports = BaseService;
