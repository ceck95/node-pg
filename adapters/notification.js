/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 12:34:24
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-11 12:14:42
 */

'use strict';

const BPromise = require('bluebird');
const BaseAdapter = require('../base/pg-adapter');


class NotificationAdapter extends BaseAdapter {


  /**
   * Filters search params to sql condition
   *
   * @param  {Object} params Search params
   * @param  {Object} opts   Options data
   *
   * @return {Object}        Query conditions
   */
  filterParams(params, pagingParams, opts) {

    opts = opts || {};
    var self = this,
      model = new self.modelClass(),
      tableAlias = model.tableAlias,
      paramCount = 1,
      where = [],
      args = [];


    if (params.title) {
      where.push('LOWER(' + tableAlias + '.title) LIKE $' + paramCount++);
      args.push('%' + params.title.toLowerCase() + '%');
    }

    if (params.message) {
      where.push('LOWER(' + tableAlias + '.message) LIKE $' + paramCount++);
      args.push(params.message.toLowerCase() + '%');
    }

    if (params.userId) {
      where.push(`(${tableAlias}.audience IS NULL OR cardinality(${tableAlias}.audience) = 0 OR ${tableAlias}.audience @> $${paramCount++}) AND (${tableAlias}.user_deleted IS NULL OR NOT ${tableAlias}.user_deleted @> $${paramCount++})`);
      args.push([params.userId], [params.userId]);
    }

    if (params.type) {
      where.push(tableAlias + '.type = $' + paramCount++);
      args.push(params.type);
    }

    if (params.subjectId) {
      where.push(tableAlias + '.subject_id = $' + paramCount++);
      args.push(params.subjectId);
    }

    if (params.status !== null) {
      where.push('status IN (' + params.status + ')');
    }

    if (params.createdFrom) {
      where.push(tableAlias + '.created_at >= $' + paramCount++);
      args.push(new Date(params.createdFrom));
    }

    if (params.createdTo) {
      where.push(tableAlias + '.created_at <= $' + paramCount++);
      args.push(new Date(params.createdTo));
    }

    return {
      where: where,
      args: args
    };
  }

  /**
   * Marks notifications as read multiple rows from table
   *
   * @param  {mixed}    pk       Primary key value
   * @param  {Function} callback Call back function
   *
   * @return {mixed}   Number of deleted row if successful. Error string if failed
   */
  markAsRead(pks, userId, opts) {

    opts = opts || {};

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName,
      primaryKeyName = model.primaryKeyName;

    return self.checkEmpty([{
      value: pks,
      message: 'updateMetadata ' + model.fullTableName + ' error: Input primary key values is null',
      source: 'pks'
    }, {
      value: userId,
      message: 'updateMetadata ' + model.fullTableName + ' error: Input userId values is null',
      source: 'userId'
    }], opts).then(() => {

      self.log.debug('Begins marking notificatons as read. Table name: ', tableName);

      let sql = `UPDATE ${tableName} SET user_read = array_append(user_read, $1) WHERE ${primaryKeyName} IN (${pks.join(', ')}) AND (user_read IS NULL OR NOT user_read @> $2)`;

      return self.query(sql, [userId, [userId]], opts).then(result => {
        self.log.info('Update successfully. Count:', result.rowCount);
        return BPromise.resolve(result.rowCount);
      });
    });
  }

  /**
   * Marks all notifications as read from table
   *
   * @param  {String}   userId   User ID
   * @param  {String}   type     Notification type
   * @param  {Object}   opts     Option data
   *
   * @return {mixed}   Number of deleted rows if successful. Error if failed
   */
  markAllAsRead(userId, type, opts) {

    opts = opts || {};

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName;

    return self.checkEmpty([{
      value: userId,
      message: 'markAllAsRead ' + model.fullTableName + ' error: Input userId values is null',
      source: 'userId'
    }], opts).then(() => {

      let sql = `UPDATE ${tableName} SET user_read = array_append(user_read, $1) WHERE (audience IS NULL OR cardinality(audience) = 0 OR audience @> $2) AND (user_read IS NULL OR NOT user_read @> $3)`,
        args = [
          userId, [userId],
          [userId]
        ];

      if (type) {
        sql += ' AND type = $4';
        args.push(type);
      }

      return self.query(sql, args, opts).then(results => {
        self.log.info('Update successfully. Count:', results.rowCount);

        return BPromise.resolve(results.rowCount);
      });
    });
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
  deleteOneByUser(pk, userId, opts) {

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName,
      pkName = model.primaryKeyName || 'uid';

    return self.checkEmpty([{
      value: pk,
      message: 'DELETE' + tableName + ' error: Input primary key is empty',
      source: 'pk'
    }], opts).then(() => {

      let sql = `UPDATE ${tableName} SET user_deleted = array_append(user_deleted, $1) WHERE (audience IS NULL OR cardinality(audience) = 0 OR audience @> $2) AND (user_deleted IS NULL OR NOT user_deleted @> $3) AND ${pkName} = $4`,
        args = [
          userId, [userId],
          [userId], pk
        ];

      return self.query(sql, args, opts).then(results => {
        self.log.info('Update user_deleted successfully. Count:', results.rowCount);

        return BPromise.resolve(results.rowCount);
      });

    });
  }

  /**
   * Count unread notifications of current user
   *
   * @param  {String}   userId   User ID
   * @param  {String}   type     Notification type
   * @param  {Object}   opts     Option data
   *
   * @return {mixed}   Number of unread rows if successful. Error if failed
   */
  countUnread(userId, type, opts) {

    opts = opts || {};

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName;

    return self.checkEmpty([{
      value: userId,
      message: 'countUnread ' + model.fullTableName + ' error: Input userId values is null',
      source: 'userId'
    }], opts).then(() => {
      let sql = `SELECT COUNT(*) AS total FROM ${tableName} WHERE (audience IS NULL OR cardinality(audience) = 0 OR audience @> $1) AND (user_read IS NULL OR NOT user_read @> $2) AND (user_deleted IS NULL OR NOT user_deleted @> $3)`,
        args = [
          [userId],
          [userId],
          [userId],
        ];

      if (type) {
        sql += ' AND type = $4';
        args.push(type);
      }

      return self.query(sql, args, opts).then(result => {
        self.log.debug('There is ' + result.rows[0].total + ' unread notifications');

        return BPromise.resolve(result.rows[0].total);
      });
    });
  }
}
module.exports = NotificationAdapter;
