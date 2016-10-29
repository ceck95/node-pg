/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 23:43:14
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-04 13:39:19
 */

'use strict';

const Hoek = require('hoek');
const config = require('config');
const BPromise = require('bluebird');
const helpers = require('node-helpers');

const BaseAdapter = require('../base/pg-adapter');
const ProvinceAdapter = require('./province');
const DistrictAdapter = require('./district');
const WardAdapter = require('./ward');

const errorHelper = helpers.Error;

const defaultCountry = config.get('i18n.country');

class AddressAdapter extends BaseAdapter {

  /**
   * Disable or enable postgis extension
   *
   * @return {Boolean}
   */
  get enablePostgis() {
    return false;
  }

  /**
   * Returns ward adapter class
   *
   * @return {WardAdapter}
   */
  get wardAdapterClass() {
    return WardAdapter;
  }

  /**
   * Returns province adapter class
   *
   * @return {WardAdapter}
   */
  get provinceAdapterClass() {
    return ProvinceAdapter;
  }

  /**
   * Returns district adapter class
   *
   * @return {DistrictAdapter}
   */
  get districtAdapterClass() {
    return DistrictAdapter;
  }

  /**
   * Before save hooking
   *
   * @param  {Object}   model    Request model
   * @param  {Object}   opts     Option data
   */
  applyAddress(model) {

    let self = this;

    if (!model.countryCode) {
      model.countryCode = defaultCountry;
    }


    let getProvince = () => {
      return new BPromise((resolve, reject) => {

        if (!model.provinceCode || (model.provinceCode && model.province)) {
          self.log.debug('No need to update province');
          return resolve(null);
        }

        let adapter = new self.provinceAdapterClass();

        return adapter.getOne({
          where: ['country_code = $1', 'province_code = $2'],
          args: [model.countryCode, model.provinceCode]
        }).then(rawProvince => {
          let province = new adapter.modelClass(rawProvince);
          model.province = province.displayName;
          return resolve(province);
        }, err => {
          return reject(err);
        });
      });
    };

    let getDistrict = () => {
      return new BPromise((resolve, reject) => {

        if (!model.provinceCode || !model.districtCode || (model.districtCode && model.district)) {
          self.log.debug('No need to update district');
          return resolve(null);
        }

        let districtAdapter = new self.districtAdapterClass();

        return districtAdapter.getOne({
          where: ['country_code = $1', 'province_code = $2', 'district_code = $3'],
          args: [model.countryCode, model.provinceCode, model.districtCode]
        }).then(rawDistrict => {

          self.log.debug('District', rawDistrict);

          let district = new districtAdapter.modelClass(rawDistrict);
          model.district = district.displayName;
          return resolve(district);

        }, err => {
          return reject(err);
        });
      });
    };

    let getWard = new BPromise((resolve, reject) => {
      if (!model.wardId || (model.wardId && model.ward)) {
        return resolve(null);
      }

      let wardAdapter = new self.wardAdapterClass();
      return wardAdapter.getOneByPk(model.wardId).then(rawWard => {
        let ward = new wardAdapter.modelClass(rawWard);
        model.ward = ward.displayName;

        if (!model.districtCode) {
          model.districtCode = ward.districtCode;
          model.district = null;
        }

        if (!model.provinceCode) {
          model.provinceCode = ward.provinceCode;
          model.province = null;
        }


        return BPromise.all([getProvince(), getDistrict()]).then(results => {
          self.log.debug('Apply province and district', results);

          return resolve(ward);

        }, err => {
          self.log.error(err);
          return reject(err);
        });
      });
    });

    return BPromise.all([getProvince(), getDistrict(), getWard]);
  }

  /**
   * Insert data into database
   *
   * @param  {object} model Input model
   * @param  {object} opts Optional settings
   * @param  {Function} callback Call back function
   *
   * @return {mixed} new created id if successful insertion, error string if failed insertion
   */
  insertOne(model, opts) {

    let self = this;

    opts = opts || {};


    return this.applyAddress(model, opts).then(() => {
      if (model.longitude && model.latitude && self.enablePostgis) {
        opts.extraColumns = [{
          key: 'gis_geometry',
          value: 'ST_MakePoint(' + model.longitude + ', ' + model.latitude + ')'
        }];
      }

      return super.insertOne(model, opts);
    });
  }

  /**
   * Update data into database
   *
   * @param  {object} model Input model
   * @param  {object} opts Optional settings
   * @param  {Function} callback Call back function
   *
   * @return {mixed} true if successful update, error string if failed insertion
   */
  updateOne(model, opts) {

    opts = opts || {};

    let self = this;

    return this.applyAddress(model).then(() => {
      if (model.userId) {
        if (!model.createdBy) {
          model.createdBy = model.userId;
        }

        if (!model.updatedBy) {
          model.updatedBy = model.userId;
        }
      }

      if (model.longitude && model.latitude && self.enablePostgis) {
        opts.extraColumns = [{
          key: 'gis_geometry',
          value: 'ST_MakePoint(' + model.longitude + ', ' + model.latitude + ')'
        }];
      }

      return super.updateOne(model, opts);
    });
  }

  /**
   * Update or insert address into database
   *
   * @param  {object} model Input model
   * @param  {object} opts Optional settings
   *
   * @return {mixed} true if successful upsertion, error string if failed insertion
   */
  upsertOne(model, opts) {

    if (model.uid) {
      return this.updateOne(model, opts);
    }

    return this.insertOne(model, opts);
  }
  /**
   * Update data into database
   *
   * @param  {object} model Input model
   * @param  {object} opts Optional settings
   *
   * @return {mixed} true if successfully, error string if failed
   */
  updateBySubject(model, opts) {

    Hoek.assert(model, 'Empty input model. Cannot update into database');

    let self = this;
    if (!model.getAttributes) {
      model = new self.modelClass(model);
    }

    let tableName = model.fullTableName;

    opts = Hoek.applyToDefaults({
      returning: true,
      model: model
    }, opts || {});

    self.checkEmpty([{
      value: model.subjectId,
      message: 'Empty subjectId',
      source: 'subjectId'
    }], opts).then(() => {

      self.log.info('Begins updating model. Table name: ', tableName, '. Subject ID: ', model.subjectId);

      return self.getBySubject(model.subjectId, model.type, opts).then(row => {
        if (!row) {
          let err = errorHelper.notFound('subjectId');
          return BPromise.reject(err);
        }

        opts.oldModel = row;
        return self.updateOne(model, opts);
      });
    });
  }

  /**
   * Query rows from table, filtered by user
   *
   * @param  {string} userId  Client ID value
   * @param  {Object} opts    Option data
   *
   * @return {BPromise}       Result promise
   */
  getByUser(userId, opts) {
    opts = opts || {};

    var self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName;

    self.checkEmpty([{
      value: userId,
      message: 'Get ' + tableName + ' error: Input user ID value is null',
      source: 'userId'
    }], opts).then(() => {

      let condition = {
        where: ['user_id = $1'],
        args: [userId]
      };

      return self.getOne(condition, opts);
    });
  }

  /**
   * Query rows from table, filtered by subject
   *
   * @param  {string} subjectId  Client ID value
   * @param  {string} type       Subject Type
   * @param  {Object} opts    Option data
   *
   * @return {BPromise}       Result promise
   */
  getOneBySubject(subjectId, type, opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName;

    self.checkEmpty([{
      value: subjectId,
      message: 'getBySubject ' + tableName + ' error: Input subjectId value is null',
      source: 'subjectId'
    }], opts).then(() => {

      let condition = {
        where: ['subject_id = $1'],
        args: [subjectId]
      };

      if (type) {
        condition.where.push('type = $2');
        condition.args.push(type);
      }

      return self.getOne(condition, opts);
    });
  }

  /**
   * Query rows from table, filtered by subject
   *
   * @param  {string} subjectId  Client ID value
   * @param  {Function} callback Call back function
   *
   * @return {object}          Result model
   */
  getManyBySubject(subjectIds, type, opts) {

    Hoek.assert(this.modelClass, 'Model of adapter has not been set');

    opts = opts || {};

    let self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName;

    self.checkEmpty([{
      value: subjectIds,
      message: 'getBySubjects ' + tableName + ' error: Input subjectIds value is ' + subjectIds,
      source: 'subjectIds'
    }], opts).then(() => {

      self.log.info('Get Address by Subjects', subjectIds, 'Type', type);

      let condition = {
        where: ['subject_id IN (' + subjectIds.join(', ') + ')'],
        args: []
      };

      if (type) {
        condition.where.push('type = $1');
        condition.args.push(type);
      }

      return self.getAllCondition(condition, opts);
    });
  }

  /**
   * Search addresses by radius range
   *
   * @param  {Object}   params   Search params
   * @param  {Object}   opts     Optional data
   */
  searchByRadius(params, opts) {

    Hoek.assert(this.enablePostgis, 'Postgis extension is disabled');

    opts = opts || {};

    let self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName,
      columns = helpers.Postgres.extractColumns(model),
      conditions = [],
      args = [];

    if (params.minDistance) {
      conditions.push('ST_Distance_Sphere(gis_geometry, ST_MakePoint($1, $2)) BETWEEN $3 AND $4');
      args = [params.geometry.coordinates[0], params.geometry.coordinates[1], params.minDistance, params.maxDistance];
    } else {
      conditions.push('ST_Distance_Sphere(gis_geometry, ST_MakePoint($1, $2)) <= $3');
      args = [params.geometry.coordinates[0], params.geometry.coordinates[1], params.maxDistance];
    }


    if (opts.conditions) {
      opts.conditions.forEach(function(element) {
        conditions.push(element.where);
        if (element.arg) {
          args.push(element.arg);
        }
      });
    }

    let sql = 'SELECT ' + columns.join(', ') + ' FROM ' + tableName + ' WHERE ' + conditions.join(' AND ') + ';';

    self.log.debug('Search address by radius. SQL:', sql, '. Args', args);

    return self.query(sql, args, opts).then(result => {

      self.log.info(result.rows.length + ' rows were received');
      return BPromise.resolve(result.rows);
    });
  }

  /**
   * Search shop by radius addresses range
   *
   * @param  {Object}   params   Search params
   * @param  {Object}   opts     Optional data
   */
  searchByRadiusType(params, opts) {

    opts = opts || {};

    var self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName,
      columns = helpers.Postgres.extractColumns(model),
      conditions = [],
      args = [];

    if (params.minDistance) {
      conditions.push('ST_Distance_Sphere(gis_geometry, ST_MakePoint($1, $2)) BETWEEN $3 AND $4');

      args = [params.geometry.coordinates[0], params.geometry.coordinates[1], params.minDistance, params.maxDistance];
    } else {
      conditions.push('ST_Distance_Sphere(gis_geometry, ST_MakePoint($1, $2)) <= $3');
      args = [params.geometry.coordinates[0], params.geometry.coordinates[1], params.maxDistance];
    }
    conditions.push('type @> $' + (args.length + 1));
    args.push(params.types);

    conditions.push('subject_id IS NOT NULL');

    let sql = 'SELECT DISTINCT ON (subject_id) ' + columns.join(', ') + ' FROM ' + tableName + ' WHERE ' + conditions.join(' AND ') + ' ORDER BY subject_id DESC;';

    self.log.debug('Search address by radius. SQL:', sql, '. Args', args);

    return self.query(sql, args, opts).then(result => {
      self.log.info(result.rows.length + ' rows were received');

      return BPromise.resolve(result.rows);
    });

  }

  /**
   * Query single row from table by subject
   * If not existed, creating new empty record
   *
   * @param  {Object} model Model input data
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getOrCreateBySubject(model, opts) {

    Hoek.assert(this.modelClass, 'Model of adapter has not been set');

    opts = opts || {};

    let self = this;

    return self.getBySubject(model.subjectId, model.type, opts).then(row => {

      if (!row) {
        self.log.debug('Profile not found. Creating profile...');
        // record not found, create new record
        return self.insertOne(model, opts);
      } else {
        return BPromise.resolve(row);
      }
    });

  }
}

module.exports = AddressAdapter;
