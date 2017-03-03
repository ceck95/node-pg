/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 21:38:29
* @Last modified by:   nhutdev
* @Last modified time: 2017-03-03T17:57:01+07:00
 */

'use strict';

const Hoek = require('hoek');
const helpers = require('node-helpers');
const pgHelpers = require('./helpers');
const BPromise = require('bluebird');
const BaseAdapter = require('./base-adapter');

class PostgresAdapter extends BaseAdapter {

  /**
   * Constructor, set default data
   */
  constructor() {
    super();

    this._model = new this.modelClass();
    this._columnNames = pgHelpers.extractColumns(this._model);
  }

  /**
   * Read-only model instance
   *
   * @return {Object}
   */
  get model() {
    return this._model;
  }

  /**
   * Read-only model instance
   *
   * @return {Object}
   */
  get columnNames() {
    return this._columnNames;
  }

  /**
   * Returns model class for current adapter
   *
   * @return {Object} Model class
   */
  get modelClass() {
    Hoek.assert(false, 'modelClass method has not been implemented');
  }



  /**
   * Insert data into database
   *
   * @param  {object} model Input model
   * @param  {object} opts Optional settings
   *
   * @return {mixed} new created id if successful insertion, error string if failed insertion
   */
  insertOne(model, opts) {

    let self = this;
    if (!model.getAttributes) {
      model = new self.modelClass(model);
    }
    let tableName = model.fullTableName;


    return self.checkEmpty([{
      value: model,
      message: 'Empty input model. Cannot insert into table ' + tableName,
      source: 'model'
    }], opts).then(() => {

      self.log.debug('Begins inserting model. Table name: ', tableName);

      opts = opts || {};

      if (opts.returning === undefined) {
        opts.returning = true;
      }

      if (model.beforeSave) {
        model.beforeSave(true);
      }

      if (model.hasOwnProperty('createdAt')) {
        model.updatedAt = helpers.Data.toUtcString(model.createdAt, null, new Date());
      } else if (model.hasOwnProperty('created')) {
        model.updated = helpers.Data.toUtcString(model.created, null, new Date());
      }

      if (model.hasOwnProperty('updatedAt')) {
        model.updatedAt = helpers.Data.toUtcString(model.updatedAt, null, new Date());
      } else if (model.hasOwnProperty('updated')) {
        model.updated = helpers.Data.toUtcString(model.updated, null, new Date());
      }

      if (model.createdBy && !model.updatedBy) {
        model.updatedBy = model.createdBy;
      }

      let sqlInsert = pgHelpers.sqlInsert(tableName, model, opts);

      self.log.debug('SQL params:', sqlInsert);

      return self.query(sqlInsert.sql, sqlInsert.args, opts).then(result => {
        self.log.debug('Insert successfully 1 row. Uid: ', result.rows[0].uid);

        if (self.afterCUD) {
          self.afterCUD(result);
        }

        if (self.afterInsertOne) {
          self.afterInsert(result);
        }

        return BPromise.resolve(result.rows[0]);
      });
    });
  }

  /**
   * Insert many rows into database
   *
   * @param  {object} models Input models
   * @param  {object} opts Optional settings
   *
   * @return {mixed} new created id if successful insertion, error string if failed insertion
   */
  insertMany(models, opts) {

    Hoek.assert(models, 'Empty input model. Cannot insert into database');

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName,
      primaryKeyName = model.primaryKeyName;

    opts = opts || {};

    opts = Hoek.applyToDefaults({
      excepts: [],
      returning: true,
      model: model
    }, opts || {});

    let insertedModels = [];
    models.forEach((element) => {
      let m = element.getAttributes ? element : new self.modelClass(element);
      m.beforeSave(true);
      insertedModels.push(m);
    });

    if (!insertedModels[0][primaryKeyName]) {
      opts.excepts.push(primaryKeyName);
    }

    let sqlInsert = pgHelpers.sqlInsertMany(tableName, insertedModels, opts);

    return self.query(sqlInsert.sql, sqlInsert.args, opts).then(result => {
      self.log.info('insertMany successfully!. Count: ', result.rows.length);

      if (self.afterCUD) {
        self.afterCUD(result.rows);
      }

      if (self.afterInsertMany) {
        self.afterInsertMany(result.rows);
      }

      return BPromise.resolve(result.rows);
    });
  }

  /**
   * Update data into database
   *
   * @param  {object} model Input model
   * @param  {object} opts Optional settings
   *
   * @return {mixed} true if successful insertion, error string if failed insertion
   */
  updateOne(model, opts) {

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

    self.log.info('Begins updating model. Table name: ', tableName, '. Uid: ', model.uid);

    return self.checkEmpty({
      value: model.uid,
      message: 'Empty primary key. Cannot update record in table ' + tableName,
      source: 'uid'
    }, opts).then(() => {

      return new BPromise((resolve, reject) => {

        let updateFunc = (row) => {

          if (model.hasOwnProperty('updatedAt')) {
            model.updatedAt = helpers.Data.toUtcString(model.updatedAt, null, new Date());
          } else if (model.hasOwnProperty('updated')) {
            model.updated = helpers.Data.toUtcString(model.updated, null, new Date());
          }

          if (model.beforeSave) {
            model.beforeSave(false);
          }

          let diffData = pgHelpers.diff(row, model);

          if (model.hasOwnProperty('metadata') && model.metadata) {
            var metadata = row.metadata || {};
            if (typeof(model.metadata) === 'string') {
              model.metadata = JSON.parse(model.metadata);
            }
            Hoek.merge(metadata, model.metadata, false, false);
            diffData.metadata = JSON.stringify(metadata);
          }

          if (model.hasOwnProperty('settings') && model.settings) {
            let settings = row.settings || {};
            if (typeof(model.settings) === 'string') {
              model.settings = JSON.parse(model.settings);
            }
            Hoek.merge(settings, model.settings, false, false);
            diffData.settings = JSON.stringify(settings);
          }

          delete diffData.created;
          delete diffData.created_at;

          if (helpers.Data.isEmpty(diffData)) {

            self.log.info('Nothing new to update');
            return resolve(row);
          }

          self.log.debug('Updates params: ', diffData);

          let where = ' WHERE uid = $' + (Object.keys(diffData).length + 1);

          opts.where = where;
          let updateSql = pgHelpers.sqlUpdate(tableName, diffData, opts);

          updateSql.args.push(model.uid);

          return self.query(updateSql.sql, updateSql.args, opts).then(result => {
            self.log.info('updateOne successfully!. Count: ', result.rowCount);

            if (self.afterCUD) {
              self.afterCUD(result);
            }

            if (self.afterUpdateOne) {
              self.afterInsertMany(result);
            }

            return resolve(result.rows[0]);
          }, err => {
            return reject(err);
          });
        };

        if (opts.oldModel) {
          return updateFunc(opts.oldModel);
        } else {

          return self.getOne(model.uid, opts).then(row => {
            return updateFunc(row);
          }, err => {
            return reject(err);
          });
        }

      });
    });
  }

  /**
   * Query single row from table
   *
   * @param  {mixed} pk Primary key value
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getOne(condition, opts) {

    Hoek.assert(this.modelClass, 'Model of adapter has not been set');

    opts = opts || {};

    let args,
      self = this,
      where = '',
      model = new this.modelClass(),
      order = '',
      tableName = model.fullTableName,
      pkName = model.primaryKeyName || 'uid',
      columns = pgHelpers.extractColumnString(model);

    return self.checkEmpty([{
      value: condition,
      message: 'Get ' + tableName + ' error: Input primary key value is null',
      source: 'condtion'
    }], opts).then(() => {

      if (typeof(condition) === 'object') {
        where = Array.isArray(condition.where) ? condition.where.join(' AND ') : condition.where;
        args = condition.args || [];
        order = condition.order || '';
      } else {
        where = pkName + ' = $1 ';
        args = [condition];
      }

      var sql = 'SELECT ' + columns + ' FROM ' + tableName;

      if (where) {
        sql += ' WHERE ' + where;
      }

      sql += pgHelpers.sqlOrder(model, order);

      sql += ' LIMIT 1';

      return self.query(sql, args, opts).then(result => {
        self.log.info(result.rows.length + ' rows were received');

        if (self.afterGetOne) {
          self.afterGetOne(result);
        }
        return BPromise.resolve(result.rows[0]);
      });
    });
  }

  /**
   * Query single row from table by primary key
   *
   * @param  {mixed}    pk Primary key value
   * @param {Object}    opts Option data
   *
   */
  getOneByPk(pk, opts) {

    let model = new this.modelClass(),
      pkName = model.primaryKeyName || 'uid';

    return this.getOne({
      where: [pkName + ' = $1 '],
      args: [pk]
    }, opts);
  }

  /**
   * Checks exists row with condition
   *
   * @param  {Object} condition Query condition
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  exists(condition, opts) {

    opts = opts || {};
    let args,
      self = this,
      where = '',
      model = new this.modelClass(),
      order = '',
      tableName = model.fullTableName,
      pkName = model.primaryKeyName || 'uid';

    return self.checkEmpty([{
      value: condition,
      message: 'Get ' + tableName + ' error: Input primary key value is null',
      source: 'condtion'
    }], opts).then(() => {

      if (typeof(condition) === 'object') {
        where = Array.isArray(condition.where) ? condition.where.join(' AND ') : condition.where;
        args = condition.args || [];
        order = condition.order || '';
      } else {
        where = pkName + ' = $1 ';
        args = [condition];
      }

      let sql = 'SELECT EXISTS (SELECT 1 FROM ' + tableName;

      if (where) {
        sql += ' WHERE ' + where;
      }
      sql += ')';

      return self.query(sql, args, opts).then(result => {
        self.log.debug('Exists:', result.rows[0].exists);
        return BPromise.resolve(result.rows[0].exists);
      });
    });
  }

  /**
   * Query all rows from table, with condition
   *
   * @param  {mixed} pk Primary key value
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAllCondition(condition, opts) {

    Hoek.assert(this.modelClass, 'Model of adapter has not been set');

    opts = opts || {};

    let args,
      self = this,
      where = '',
      model = new this.modelClass(),
      order = '',
      tableName = model.fullTableName,
      columns = pgHelpers.extractColumnString(model);

    return self.checkEmpty([{
      value: condition,
      message: 'getAllCondition ' + tableName + ' error: Input primary key value is null',
      source: 'condtion'
    }], opts).then(() => {

      where = Array.isArray(condition.where) ? condition.where.join(' AND ') : condition.where;
      args = condition.args || [];
      order = condition.order || '';

      let sql = 'SELECT ' + columns + ' FROM ' + tableName + ' WHERE ' + where +
        pgHelpers.sqlOrder(model, order, {
          hasRelation: false
        });

      return self.query(sql, args, opts).then(result => {
        self.log.info(result.rows.length + ' rows were received');
        return BPromise.resolve(result.rows);
      });
    });
  }

  /**
   * Query all row from table with condition and relation
   *
   * @param {Object}    condition Condition data
   * @param  {Object} opts Optional data
   *
   * @return {Object}   Result model
   */
  getAllConditionRelation(condition, opts) {

    opts = opts || {};
    if (Array.isArray(opts)) {
      opts = {
        includes: opts
      };
    }

    let self = this,
      where = '',
      args = [],
      order = '',
      model = new this.modelClass(),
      tableName = model.fullTableName,
      tableAlias = model.tableAlias;

    return self.checkEmpty([{
      value: condition,
      message: 'Get ' + tableName + ' error: Input condition is empty',
      source: 'condition'
    }], opts).then(() => {

      where = Array.isArray(condition.where) ? condition.where : condition.where.split(' AND ');
      args = condition.args || [];
      order = condition.order || opts.order || '';

      let relation = self.sqlRelation(model, opts, where, args),
        columns = pgHelpers.extractColumnString(relation.includes),
        sql = 'SELECT ' + columns + ' FROM ' + tableName + ' ' + tableAlias;

      if (relation.joins) {
        sql += ' ' + relation.joins.join(' ');
      }

      if (relation.where) {
        sql += ' WHERE ' + relation.where.join(' AND ');
      }

      sql += pgHelpers.sqlOrder(model, order, {
        hasRelation: true
      });

      return self.query(sql, relation.args, opts).then(result => {
        self.log.info(result.rows.length + ' rows were received');
        return BPromise.resolve(result.rows);
      });
    });
  }


  /**
   * Query single row from table with relation
   *
   * @param {Object}    opts Option data
   * @param  {Object} opts Optional data
   *
   * @return {Object}   Result model
   */
  getOneRelation(condition, opts) {

    opts = opts || {};
    if (Array.isArray(opts)) {
      opts = {
        includes: opts
      };
    }

    let self = this,
      where = '',
      args = [],
      order = '',
      model = new this.modelClass(),
      tableName = model.fullTableName,
      tableAlias = model.tableAlias,
      pkName = model.primaryKeyName || 'uid';

    return self.checkEmpty([{
      value: condition,
      message: 'Get ' + tableName + ' error: Input condition is empty',
      source: 'condition'
    }], opts).then(() => {

      if (typeof(condition) === 'object') {
        where = Array.isArray(condition.where) ? condition.where : condition.where.split(' AND ');
        args = condition.args || [];
        order = condition.order || opts.order || '';
      } else {
        where = [tableAlias + '.' + pkName + ' = $1 '];
        args = [condition];
        order = opts.order || '';
      }

      let relation = self.sqlRelation(model, opts, where, args),
        columns = pgHelpers.extractColumnString(relation.includes),
        sql = 'SELECT ' + columns + ' FROM ' + tableName + ' ' + tableAlias;

      if (relation.joins) {
        sql += ' ' + relation.joins.join(' ');
      }

      if (relation.where) {
        sql += ' WHERE ' + relation.where.join(' AND ');
      }

      sql += pgHelpers.sqlOrder(model, order, {
        hasRelation: true
      }) + ' LIMIT 1';

      return self.query(sql, relation.args, opts).then(result => {
        self.log.info(result.rows.length + ' rows were received');
        return BPromise.resolve(result.rows[0]);
      });
    });
  }

  /**
   * Query single row from table with relation
   *
   * @param  {mixed}    pk Primary key value
   * @param {Object}    opts Option data
   *
   */
  getOneRelationByPk(pk, opts) {

    let model = new this.modelClass(),
      tableAlias = model.tableAlias,
      pkName = model.primaryKeyName || 'uid';

    return this.getOneRelation({
      where: [tableAlias + '.' + pkName + ' = $1 '],
      args: [pk]
    }, opts);
  }

  /**
   * Deletes with condition
   *
   * @param  {Object}   condition Query where condition
   * @param  {Object} opts Optional data
   *
   * @return {mixed}   Number of deleted row if successful. Error object if failed
   */
  deleteMany(condition, opts) {

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName;

    condition = condition || {};
    opts = opts || {};

    return self.checkEmpty([{
      value: condition,
      message: 'DELETE ' + tableName + ' error: Input condition is empty',
      source: 'condition'
    }], opts).then(() => {

      let where = Array.isArray(condition.where) ? condition.where.join(' AND ') : condition.where,
        args = condition.args || [],
        sql = 'DELETE FROM ' + tableName + (where ? ' WHERE ' + where : '');

      return self.query(sql, args, opts).then(result => {
        self.log.info('DELETE ' + tableName + ' successfully. Count:', result.rowCount);

        if (self.afterCUD) {
          self.afterCUD(result);
        }

        if (self.afterDelete) {
          self.afterDelete(result);
        }
        return BPromise.resolve(result.rows[0]);
      });
    });
  }

  /**
   * Delete single row from table
   *
   * @param  {mixed}    pk       Primary key value
   * @param  {Object} opts Optional data
   *
   * @return {mixed}   Number of deleted row if successful. Error string if failed
   */
  deleteByPk(pk, opts) {

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName,
      pkName = model.primaryKeyName || 'uid';

    return self.checkEmpty([{
      value: pk,
      message: 'DELETE' + tableName + ' error: Input primary key is empty',
      source: 'pk'
    }], opts).then(() => {

      return self.deleteMany({
        where: [pkName + ' = $1'],
        args: [pk]
      }, opts);
    });
  }

  /**
   * Query many rows from list of primary keys, with relations
   *
   * @param  {Array} pks Primary keys value
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getMany(pks, opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName;

    return self.checkEmpty([{
      value: pks,
      message: 'GetMany ' + tableName + ' error: Input condition is empty',
      source: 'pks'
    }], opts).then(() => {
      let columns = pgHelpers.extractColumnString(model),
        pkName = model.primaryKeyName || 'uid',
        where = [pkName + ' IN (' + pks.join(',') + ')'];

      let sql = 'SELECT ' + columns + ' FROM ' + tableName + ' WHERE ' + where.join(' AND ');

      return self.query(sql, [], opts).then(result => {
        self.log.info(result.rows.length + ' rows were received');
        return BPromise.resolve(result.rows);
      });
    });
  }

  /**
   * Query all rows from database, without relations
   *
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAll(opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName,
      columns = pgHelpers.extractColumnString(model);

    let sql = 'SELECT ' + columns + ' FROM ' + tableName;

    sql += pgHelpers.sqlOrder(model);

    return self.query(sql, [], opts).then(results => {
      self.log.info(results.rows.length + ' rows were received');
      return BPromise.resolve(results.rows);
    });
  }

  /**
   * Query all rows from database, without relations
   * Filtered by status
   *
   * @param  {mixed} status Status
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAllStatus(status, opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName,
      columns = pgHelpers.extractColumnString(model);

    return self.checkEmpty([{
      value: status,
      message: 'Get ' + tableName + ' error: Model has not status property',
      source: 'status'
    }], opts).then(() => {
      let sql = 'SELECT ' + columns + ' FROM ' + tableName + ' WHERE status = $1',
        args = [status];

      sql += pgHelpers.sqlOrder(model);

      return self.query(sql, args, opts).then(results => {
        self.log.info(results.rows.length + ' rows were received');
        return BPromise.resolve(results.rows);
      });
    });
  }

  /**
   * Query all active rows from database, without relations
   *
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAllActive(opts) {
    return this.getAllStatus(pgHelpers.STATUS.ACTIVE, opts);
  }

  /**
   * Query all inactive rows from database, without relations, with order
   *
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAllInactive(opts) {
    return this.getAllStatus(pgHelpers.STATUS.INACTIVE, opts);
  }

  /**
   * Query all disabled rows from database, without relations, with order
   *
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAllDisabled(opts) {
    return this.getAllStatus(pgHelpers.STATUS.DISABLED, opts);
  }

  /**
   * Query all deleted rows from database, without relations, with order
   *
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAllDeleted(opts) {
    return this.getAllStatus(pgHelpers.STATUS.DELETED, opts);
  }

  /**
   * Query all rows from database, without relations, with order
   *
   * @param  {String} order Order SQL string
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getAllOrder(order, opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName;

    return self.checkEmpty([{
      value: order,
      message: 'Get ' + tableName + ' error: Order is empty',
      source: 'status'
    }], opts).then(() => {

      let columns = pgHelpers.extractColumnString(model),
        sql = 'SELECT ' + columns + ' FROM ' + tableName + ' ORDER BY ' + order;

      return self.query(sql, [], opts).then(results => {
        self.log.info(results.rows.length + ' rows were received');
        return BPromise.resolve(results.rows);
      });
    });
  }

  /**
   * Query all rows from database, with relationship and pagination
   *
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getPagination(pagingParams, opts) {

    pagingParams = pagingParams || {};
    let hasRelationship = !helpers.Array.isEmpty(pagingParams.includes);
    Hoek.assert(!hasRelationship || !!this.sqlRelation, 'sqlRelation method has not been implemented');

    opts = opts || {};

    let sql, args, whereSql,
      self = this,
      model = new this.modelClass(),
      tableAlias = model.tableAlias,
      tableName = model.fullTableName + ' ' + model.tableAlias,
      columns = [],
      paging = pgHelpers.sqlPagination(pagingParams, {
        alias: tableAlias
      }),
      sqlCount = 'SELECT COUNT(*) AS total FROM ' + tableName;


    if (hasRelationship) {
      let relation = self.sqlRelation(model, pagingParams);

      columns = pgHelpers.extractColumnString(relation.includes);

      sql = 'SELECT ' + columns + ' FROM ' + tableName + relation.joins.join(' ');

      if (relation.where.length > 0) {
        whereSql = ' WHERE ' + relation.where.join(' AND ');
        sqlCount += whereSql;
        sql += whereSql;
      }

      args = relation.args;
    } else {
      columns = pgHelpers.extractColumns(model);
      sql = 'SELECT ' + columns.join(', ') + ' FROM ' + tableName;
      args = [];
    }

    sql += pgHelpers.sqlOrder(model, pagingParams.order, {
      hasRelation: hasRelationship
    });


    return self.query(sqlCount, args, opts).then(result => {

      let totalRows = result.rows[0].total,
        pageSize = pagingParams.pageSize > 0 ? pagingParams.pageSize : totalRows,
        totalPages = (totalRows % pageSize === 0) ? (totalRows / pageSize) : ((totalRows / pageSize) + 1);

      self.log.info('Total rows: ', totalRows);

      let resp = {
        meta: {
          pageSize: pageSize,
          pageNumber: paging.pageNumber,
          totalPages: totalPages,
          total: totalRows,
        },
      };

      if (paging.pageIndex > totalRows) {
        resp.data = [];
        resp.meta.count = 0;
        return BPromise.resolve(resp);
      }

      if (paging.sql !== false) {
        sql += ' ' + paging.sql;
      }

      return self.query(sql, args, opts).then(results => {
        self.log.info(results.rows.length + ' rows were received');
        resp.meta.count = results.rows.length;
        resp.data = results.rows;

        return BPromise.resolve(resp);
      });

    });
  }


  /**
   * Query many rows from list of primary keys, with relations
   *
   * @param  {Array} pks Primary keys value
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getManyRelation(pks, opts) {

    opts = opts || {};

    if (Array.isArray(opts)) {
      opts = {
        includes: opts
      };
    }

    let self = this,
      model = new this.modelClass(),
      tableAlias = model.tableAlias,
      tableName = model.fullTableName + ' ' + model.tableAlias,
      columns = [],
      where = [tableAlias + '.uid IN (' + pks.join(',') + ')'];

    let relation = self.sqlRelation(model, opts, where);

    columns = pgHelpers.extractColumnString(relation.includes);

    let sql = 'SELECT ' + columns + ' FROM ' + tableName + relation.joins.join(' ') + ' WHERE ' + relation.where.join(' AND ');

    return self.query(sql, relation.args, opts).then(results => {
      self.log.info(results.rows.length + ' rows were received');
      return BPromise.resolve(results.rows);
    });
  }

  /**
   * Get multiple rows from table, with condition
   *
   * @param  {object} params Query params
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  filter(params, pagingParams, opts) {

    opts = opts || {};

    let sql, args, whereSql,
      self = this,
      model = new this.modelClass(),
      tableName = model.fullTableName + ' ' + model.tableAlias,
      columns = [],
      filteredSql = this.filterParams(params, opts),
      hasRelation = !helpers.Array.isEmpty(pagingParams.includes);

    if (hasRelation) {
      let relation = self.sqlRelation(model, opts, filteredSql.where, filteredSql.args);

      columns = pgHelpers.extractColumnString(relation.includes);

      sql = 'SELECT ' + columns + ' FROM ' + tableName + relation.joins.join(' ');

      if (relation.where.length > 0) {
        whereSql = ' WHERE ' + relation.where.join(' AND ');
        sql += whereSql;
      }
      args = relation.args;
    } else {
      columns = pgHelpers.extractColumns(model);
      sql = 'SELECT ' + columns.join(', ') + ' FROM ' + tableName;
      if (filteredSql.where.length > 0) {
        whereSql = ' WHERE ' + filteredSql.where.join(' AND ');
        sql += whereSql;
      }

      args = filteredSql.args;
    }

    sql += pgHelpers.sqlOrder(model, opts.order, {
      hasRelation: hasRelation
    });


    return self.query(sql, args, opts).then(results => {
      self.log.info(results.rows.length + ' rows were received');
      return BPromise.resolve(results.rows);
    });
  }

  /**
   * Filters multiple rows from table
   *
   * @param  {object} params Query params
   * @param  {object} pagingParams Paging params
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  filterPagination(params, pagingParams, opts) {

    Hoek.assert(this.filterParams, 'filterParams method has not been implemented');

    opts = opts || {};

    let sql, args, whereSql,
      self = this,
      model = new this.modelClass(),
      tableAlias = model.tableAlias,
      tableName = model.fullTableName + ' ' + model.tableAlias,
      columns = [],
      filteredSql = this.filterParams(params, pagingParams, opts),
      paging = pgHelpers.sqlPagination(pagingParams, {
        alias: tableAlias
      }),
      isDone = opts.isDone,
      sqlCount = 'SELECT COUNT(*) AS total FROM ' + tableName;


    if (self.sqlRelation) {
      var relation = self.sqlRelation(model, pagingParams, filteredSql.where, filteredSql.args);

      columns = pgHelpers.extractColumnString(relation.includes);

      sql = 'SELECT ' + columns + ' FROM ' + tableName + relation.joins.join(' ');

      if (relation.where.length > 0) {
        whereSql = ' WHERE ' + relation.where.join(' AND ');
        sqlCount += relation.joins.join(' ') + whereSql;

        sql += whereSql;
      }
      args = relation.args;
    } else {
      columns = pgHelpers.extractColumnString(model);
      sql = 'SELECT ' + columns + ' FROM ' + tableName;
      if (filteredSql.where.length > 0) {
        whereSql = ' WHERE ' + filteredSql.where.join(' AND ');
        sqlCount += whereSql;
        sql += whereSql;
      }

      args = filteredSql.args;
    }

    sql += pgHelpers.sqlOrder(model, pagingParams.order, {
      hasRelation: true
    });

    return self.query(sqlCount, args, opts).then(result => {
      let totalRows = result.rows[0].total,
        pageSize = pagingParams.pageSize > 0 ? pagingParams.pageSize : totalRows,
        totalPages = (totalRows % pageSize === 0) ? (totalRows / pageSize) : ((totalRows / pageSize) + 1);
      self.log.info('Total rows: ', totalRows);


      let resp = {
        meta: {
          pageSize: pageSize,
          pageNumber: paging.pageNumber,
          totalPages: totalPages,
          total: totalRows,
        },
      };

      if (paging.pageIndex > totalRows) {
        resp.data = [];
        resp.meta.count = 0;
        return BPromise.resolve(resp);
      }

      if (paging.sql !== false) {
        sql += ' ' + paging.sql;
      }

      opts.isDone = isDone;

      return self.query(sql, args, opts).then(results => {
        self.log.info(results.rows.length + ' rows were received');
        resp.meta.count = results.rows.length;
        resp.data = results.rows;

        return BPromise.resolve(resp);
      });
    });
  }

  /**
   * Query single row from table
   * If not existed, creating new empty record
   *
   * @param  {Object} model Model input data
   * @param  {Object} opts Optional data
   *
   * @return {object}   Result model
   */
  getOrCreate(model, opts) {

    Hoek.assert(this.modelClass, 'Model of adapter has not been set');

    opts = opts || {};

    let self = this;

    self.log.debug('Begining get or create model');

    if (model.uid) {

      return self.getOneByPk(model.uid, opts).then(row => {

        if (!row) {
          self.log.debug('Profile not found. Creating profile...');
          // record not found, create new record
          return self.insertOne(model, opts);
        } else {

          return BPromise.resolve(row);
        }
      });
    } else {

      return self.insertOne(model, opts);
    }
  }
}

module.exports = PostgresAdapter;
