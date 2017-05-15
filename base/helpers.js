/*
 * @Author: toan.nguyen
 * @Date:   2016-05-11 07:16:52
 * @Last Modified by: nhutdev
 * @Last Modified time: 2017-05-15 14:57:20
 */

'use strict';

const Hoek = require('hoek');
const helpers = require('node-helpers');
const errors = helpers.Error;
const stringer = helpers.Stringer;

class PostgresHelper {

  /**
   * Translate Postgres error to Nexx error
   *
   * @param  {Object} err Postgre error object
   * @return {[type]}     [description]
   */
  static translateError(dest, err) {
    Hoek.assert(err, 'Error object must not be empty');

    switch (err.code) {
      case '23505':
        dest.code = '201';
        break;
      case '23502':
        dest.code = '203';
        break;
      default:
        dest.code = err.code;
    }

    dest.message = err.detail || err.message || '';
    dest.uiMessage = errors.getUiMessage(dest.code) || dest.message;
    dest.source = err.column || err.source || '';

  }

  /**
   * Diffs 2 object in same table schema
   *
   * @param  {object} row  Source data row
   * @param  {object} data Compared data
   * @param  {object} opts Option data
   *
   * @return {object}      Diff data
   */
  static diff(row, data, opts) {
    opts = opts || {};
    opts = Hoek.applyToDefaults({
      acceptNull: false,
      acceptZero: true,
      excepts: []
    }, opts);

    let results = {};
    for (let key in row) {
      let rowKey = key,
        dataKey = key,
        hasOwnProperty = data.hasOwnProperty(dataKey);

      if (!hasOwnProperty) {
        dataKey = stringer.underscoreToCamelCase(key);
        hasOwnProperty = data.hasOwnProperty(dataKey);
      }

      if (hasOwnProperty && typeof (data[dataKey]) !== 'object') {
        let isZero = data[dataKey] === 0;
        if (row[rowKey] != data[dataKey]
          && (opts.acceptNull ? true : ((data[dataKey] || (opts.acceptZero && isZero) !== null) && (typeof (data[dataKey]) === 'string' ? data[dataKey].trim() !== '' : true)))
          && (opts.excepts.indexOf(data[dataKey]) === -1)) {
          results[rowKey] = data[dataKey];
        }
      }
    }

    return results;
  }

  /**
   * Extracts columns from model
   *
   * @param  {Object/Array} models Model or model dictionary
   * @param {Object} opts Optional settings
   *
   * @return {Array}         Array column names
   */
  static extractColumns(models, opts) {

    opts = Hoek.applyToDefaults({
      excepts: []
    }, opts || {});

    let columns = [];
    if (Array.isArray(models)) {
      models.forEach(element => {
        let alias = element.alias,
          model = element.model,
          attrs = model.getAttributes ? model.getAttributes() : model;

        for (let key in attrs) {
          let columnName = alias + '.' + key,
            columnAlias = alias + '_' + key;

          if (opts.excepts.indexOf(columnName) === -1) {
            columns.push(columnName + ' AS ' + columnAlias);
          }
        }
      });
    } else if (typeof (models) === 'object') {
      let attributes = models.getAttributes ? models.getAttributes() : models;

      for (let key in attributes) {
        if (opts.excepts.indexOf(key) === -1) {
          columns.push(key);
        }
      }
    } else {
      Hoek.assert('Invalid input model type');
    }

    return columns;
  }

  /**
   * Builds column string from model
   *
   * @param  {Object} models Input models
   * @param  {Object} opts  Option data
   *
   * @return {String}       Column string
   */
  static extractColumnString(models, opts) {

    opts = Hoek.applyToDefaults({
      excepts: []
    }, opts || {});

    let columns = [];
    if (Array.isArray(models)) {
      models.forEach(element => {
        let alias = element.alias,
          model = element.model,
          attrs = model.getAttributes ? model.getAttributes() : model;

        for (let key in attrs) {
          let columnName = '"' + alias + '"."' + key + '"',
            columnAlias = alias + '_' + key;

          if (opts.excepts.indexOf(columnName) === -1) {
            columns.push(columnName + ' AS ' + columnAlias);
          }
        }
      });

      return columns.join(', ');
    } else if (typeof (models) === 'object') {
      let attributes = models.getAttributes ? models.getAttributes() : models;

      for (let key in attributes) {
        if (opts.excepts.indexOf(key) === -1) {
          columns.push(key);
        }
      }

      return '"' + columns.join('", "') + '"';
    } else {
      Hoek.assert('Invalid input model type');
    }
  }

  /**
   * Extracts columns and args from model
   *
   * @param  {Object/Array} models Model or model dictionary
   * @param {Object} opts Optional settings
   *
   * @return {Array}         Array column names
   */
  static extractModel(models, opts) {

    opts = Hoek.applyToDefaults({
      excepts: [],
      ignoreEmpty: false
    }, opts || {});

    let results = {
        columns: [],
        args: [],
        argNames: []
      },
      paramCount = 1;



    if (Array.isArray(models)) {
      models.forEach(function(element) {
        let alias = element.alias,
          model = element.model;

        let attrs = model.getAttributes ? model.getAttributes() : model;

        for (let key in attrs) {

          if (opts.ignoreEmpty && !attrs[key] && attrs[key] !== 0 && attrs[key] !== false) {
            continue;
          }

          let columnName = alias + '.' + key,
            columnAlias = alias + '_' + key;

          if (opts.excepts.indexOf(columnName) === -1) {

            results.columns.push(columnName + ' AS ' + columnAlias);
            results.argNames.push('$' + paramCount++);
            results.args.push(attrs[key]);
          }
        }
      });
    } else if (typeof (models) === 'object') {
      let attributes = models.getAttributes ? models.getAttributes() : models;

      for (let key in attributes) {
        if (opts.ignoreEmpty && !attributes[key] && attributes[key] !== 0 && attributes[key] !== false) {
          continue;
        }

        if (opts.excepts.indexOf(key) === -1) {
          results.columns.push(key);
          results.argNames.push('$' + paramCount++);
          results.args.push(attributes[key]);
        }
      }
    } else {
      Hoek.assert('Invalid input model type');
    }

    return results;
  }

  /**
   * Generate insert query
   *
   * @param  {string} tableName table name
   * @param  {object} model     insert model
   * @param  {object} opts     Optional params
   *
   * @return {object}           sql and args
   */
  static sqlInsert(tableName, model, opts) {

    Hoek.assert(tableName, 'Table name is empty');
    Hoek.assert(model, 'Insert model is empty');

    opts = Hoek.applyToDefaults({
      excepts: [],
      extraColumns: []
    }, opts || {});

    if (model.beforeSave) {
      model.beforeSave(true);
    }

    opts.excepts = opts.excepts.concat(model.ignoreOnSave, model.ignoreOnInsert);

    let paramCount = 1,
      columns = [],
      argNames = [],
      args = [],
      returnColumns = [];

    let sql = 'INSERT INTO ' + tableName;

    let attributes = model.getAttributes ? model.getAttributes() : model;

    for (let key in attributes) {

      let value = attributes[key];
      returnColumns.push(key);

      if (value || value === 0 || value === false) {

        columns.push(key);
        argNames.push('$' + paramCount++);
        args.push(attributes[key]);
      }
    }

    if (opts.extraColumns.length > 0) {
      opts.extraColumns.forEach((element) => {
        returnColumns.push(element.key);
        columns.push(element.key);
        if (element.value) {
          argNames.push(element.value);
        } else if (element.arg) {
          argNames.push('$' + paramCount++);
          args.push(element.arg);
        }
      });
    }

    sql += '(' + columns.join(', ') + ') VALUES (' + argNames.join(', ') + ')';

    if (opts.returning) {
      if (opts.returning === true) {
        sql += ` RETURNING "${returnColumns.join('", "')}"`;
      } else if (typeof (opts.returning) === 'string') {
        sql += ' RETURNING ' + opts.returning;
      }

    }
    sql += ';';

    return {
      sql: sql,
      args: args,
      count: paramCount
    };
  }

  /**
   * Generate insert query for many models
   *
   * @param  {string} tableName table name
   * @param  {object} models     insert models
   * @param  {object} opts     Optional params
   *
   * @return {object}           sql and args
   */
  static sqlInsertMany(tableName, models, opts) {

    Hoek.assert(tableName, 'Table name is empty');
    Hoek.assert(models, 'Insert model is empty');

    opts = Hoek.applyToDefaults({
      returning: true,
      excepts: [],
      extraColumns: [],
      returnColumns: []
    }, opts || {});


    let paramCount = 1,
      columns = [],
      returnColumns = opts.returnColumns,
      argGroups = [],
      args = [],
      sql = 'INSERT INTO ' + tableName;


    // get column names first
    let firstModel = models[0],
      attributes = firstModel.getAttributes ? firstModel.getAttributes() : firstModel;

    opts.excepts = opts.excepts.concat(firstModel.ignoreOnSave, firstModel.ignoreOnInsert);

    for (let key in attributes) {
      if (opts.excepts.indexOf(key) === -1) {
        columns.push(key);
      }
    }

    if (!returnColumns ? true : returnColumns.length === 0) {
      returnColumns = PostgresHelper.extractColumns(firstModel);
    }

    models.forEach((model) => {
      attributes = model.getAttributes ? model.getAttributes() : model;

      if (model.beforeSave) {
        model.beforeSave(true);
      }
      let argNames = [];
      for (let key in attributes) {
        if (opts.excepts.indexOf(key) === -1) {
          argNames.push('$' + (args.length + 1));
          args.push(attributes[key]);
        }
      }

      argGroups.push('(' + argNames.join(', ') + ')');

    });



    sql += '(' + columns.join(', ') + ') VALUES ' + argGroups.join(', ');

    if (opts.returning) {
      if (opts.returning === true) {
        sql += ` RETURNING "${returnColumns.join('", "')}"`;
      } else if (typeof (opts.returning) === 'string') {
        sql += ' RETURNING ' + opts.returning;
      }
    }

    return {
      sql: sql,
      args: args,
      count: paramCount
    };
  }

  /**
   * Generate update query
   *
   * @param  {string} tableName table name
   * @param  {object} params    update params
   * @param  {object} opts      Optional params
   *
   * @return {object}           sql and args
   */
  static sqlUpdate(tableName, params, opts) {

    Hoek.assert(tableName, 'Table name is empty');
    Hoek.assert(params, 'Update param is empty');

    let where = '',
      defaultOpts = {
        excepts: [],
        extraColumns: []
      };

    if (opts) {
      if (typeof (opts) === 'string') {
        where = opts;
        opts = defaultOpts;
      } else {
        opts = Hoek.applyToDefaults(defaultOpts, opts);
        where = opts.where;
      }

    } else {
      opts = defaultOpts;
    }

    let paramCount = 1,
      paramTexts = [],
      args = [];

    opts.excepts = opts.excepts.concat(opts.model.ignoreOnSave, opts.model.ignoreOnUpdate);

    let sql = 'UPDATE ' + tableName + ' SET ';

    for (let key in params) {
      paramTexts.push(key + ' = $' + paramCount++);
      args.push(params[key]);
    }

    if (opts.extraColumns.length > 0) {
      opts.extraColumns.forEach(function(element) {
        if (element.value) {
          paramTexts.push(element.key + ' = ' + element.value);

        } else if (element.arg) {
          paramTexts.push(element.key + ' = ' + paramCount++);
          args.push(element.arg);
        }
      });
    }

    sql += paramTexts.join(', ') + ' ' + where;
    if (opts.returning === true && opts.model) {
      let attributes = opts.model.getAttributes ? opts.model.getAttributes() : opts.model,
        keys = Object.keys(attributes);

      if (keys.length > 0) {
        sql += ` RETURNING "${keys.join('", "')}"`;
      }


    } else if (typeof (opts.returning) === 'string') {
      sql += ' RETURNING ' + opts.returning;
    }
    sql += ';';
    return {
      sql: sql,
      args: args,
      count: paramCount
    };
  }

  /**
   * Generate pagination query
   *
   * @param  {Object} params  Pagination params
   * @param  {object} opts      Optional params
   *
   * @return {object}         Pagination condition and page index
   */
  static sqlPagination(params, opts) {

    let paging = '',
      pageNumber = 1,
      pageIndex = 0;
    opts = opts || {};

    let emptyFunc = () => {
      return {
        sql: false,
        pageNumber: pageNumber,
        pageIndex: pageIndex
      };
    };



    if (!params) {
      return emptyFunc();
    }

    if (params.paging) {
      params = params.paging;
    }

    if (params.pageNumber) {
      pageNumber = params.pageNumber < 1 ? 1 : params.pageNumber;
    }

    if (params.pageSize ? params.pageSize <= 0 : true) {
      return emptyFunc();
    }

    if (pageNumber === 1) {
      paging = ' LIMIT ' + params.pageSize;
    } else {
      pageIndex = (pageNumber - 1) * params.pageSize;
      paging = ' OFFSET ' + pageIndex + ' LIMIT ' + params.pageSize;
    }

    return {
      sql: paging,
      pageNumber: pageNumber,
      pageIndex: pageIndex
    };
  }

  /**
   * Generates SQL order from order string
   *
   * @param {Object} model  Target model
   * @param  {mixed}  order       Order data
   * @param  {String} opts        Option object
   *
   * @return {Boolean/String}     Returns false if there isn't any order
   */
  static sqlOrder(model, order, opts) {

    opts = Hoek.applyToDefaults({
      hasRelation: false
    }, opts || {});

    let strFunc = (params) => {
      let results = new Array(params.length),
        alias = opts.hasRelation ? (model.tableAlias + '.') : '';

      for (let i = params.length - 1; i >= 0; i--) {
        let element = params[i];
        if (element.indexOf('.') !== -1) {
          if (element.substr(0, 1) === '-') {
            results[i] = element.substr(1) + ' DESC';
          } else {
            results[i] = element + ' ASC';
          }
        } else {
          if (element.substr(0, 1) === '-') {
            results[i] = alias + element.substr(1) + ' DESC';
          } else {
            results[i] = alias + element + ' ASC';
          }
        }

      }

      return ' ORDER BY ' + results.join(', ');
    };

    let defaultOrder = model.defaultOrder;

    if (!order) {
      if (defaultOrder) {
        order = defaultOrder;
      } else {
        return '';
      }

    }
    let orderType = typeof (order);
    switch (orderType) {
      case 'string':
        let orderParams = order.split(',');
        return strFunc(orderParams);
      case 'object':

        if (Array.isArray(order)) {
          return strFunc(order);
        }

        let results = [],
          alias = opts.hasRelation ? (model.tableAlias + '.') : '';
        for (let k in order) {
          results.push(alias + k + ' ' + (order[k] ? 'ASC' : 'DESC'));
        }

        return ' ORDER BY ' + results.join(', ');
      default:
        Hoek.assert(false, 'Invalid SQL order type: ' + orderType);
    }
  }

  /**
   * Converts array data to SQL string
   *
   * @param  {Array} input Array input data
   *
   * @return {String}       output SQL string
   */
  static toArrayString(input) {
    Hoek.assert(Array.isArray(input), 'Input data is not array format');
    Hoek.assert(input.length > 0, 'Input data is not allowed to be empty');

    if (helpers.Data.isNumeric(input)) {
      return input.join(', ');
    }

    return "'" + input.join("', '") + "'";
  }

  /**
   * Sql limit
   */

  static sqlLimit(condition, opts) {

    Hoek.assert(condition, '[Helper Postgres]Cannot empty condition sql limit');
    opts = opts || {};

    if (condition.limit ? !helpers.Data.isEmpty(condition.limit) : false) {

      if (helpers.Data.isNumeric(condition.limit)) {
        return ` LIMIT ${condition.limit}`;
      }
      return '';

    }
    return '';
  }
}

module.exports = PostgresHelper;
module.exports.STATUS = helpers.Const.status;
