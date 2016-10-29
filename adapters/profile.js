/*
 * @Author: toan.nguyen
 * @Date:   2016-04-18 23:43:14
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-14 17:42:54
 */

'use strict';

const Hoek = require('hoek');
const BPromise = require('bluebird');
const helpers = require('node-helpers');
const pgHelpers = require('../base/helpers');
const BaseAdapter = require('../base/pg-adapter');

const errorHelper = helpers.Error;


class ProfileAdapter extends BaseAdapter {

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

    return self._checkInsertUnique(model, opts).then(() => {

      if (self.addressAdapter) {
        if (model.address) {
          let address = model.address.toThriftInsert ? model.address : new self.addressAdapter.modelClass(model.address);

          address.userId = model.uid;
          address.createdBy = model.uid;
          address.updatedBy = model.uid;
          address.type = 'profile';
          address.isDefault = true;
          address.status = 1;

          // inserted plain address
          return self.addressAdapter.insertOne(address, opts).then(insertedAddress => {

            model.addressId = insertedAddress.uid;
            return super.insertOne(model, opts).then(insertedProfile => {
              let profile = new self.modelClass(insertedProfile);

              profile.address = new self.addressAdapter.modelClass(insertedAddress);
              return BPromise.resolve(profile);
            });
          });
        } else {
          return super.insertOne(model, opts);
        }
      } else {
        return super.insertOne(model, opts);
      }
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

    let self = this;

    if (this.addressAdapter) {
      if (model.address) {
        let address = model.address.toThriftInsert ? model.address : new self.addressAdapter.modelClass(model.address);

        address.userId = model.uid;
        address.createdBy = model.uid;
        address.updatedBy = model.uid;
        address.type = 'profile';
        address.isDefault = true;
        address.status = 1;

        // inserted plain shop address
        return self.addressAdapter.upsertOne(address, opts).then(updatedAddress => {
          model.addressId = updatedAddress.uid;
          return super.updateOne(model, opts).then(updatedProfile => {
            let profile = new self.modelClass(updatedProfile);
            profile.address = new self.addressAdapter.modelClass(updatedAddress);
            return BPromise.resolve(profile);
          });
        });
      } else {
        return super.updateOne(model, opts);
      }
    } else {
      return super.updateOne(model, opts);
    }

  }

  /**
   * Checks if inserted model is unique
   *
   * @param  {Object}   model    Model data
   * @param  {Object}   opts    Option data
   */
  _checkInsertUnique(model, opts) {

    let self = this;
    if (!model.getAttributes) {
      model = new self.modelClass(model);
    }

    opts = opts || {};
    let select = [],
      args = [],
      index = args.length + 1,
      ignoreEmail = false,
      ignoreUsername = false,
      ignorePhone = false,
      localePhoneNumber = helpers.Data.removePhoneCode(model.phoneNumber, model.phoneCode),
      interPhoneNumber = helpers.Data.applyPhoneCode(model.phoneNumber, model.phoneCode);

    return new BPromise((resolve, reject) => {
      if (model.hasOwnProperty('email') && model.email) {
        select.push(`(SELECT EXISTS (SELECT 1 FROM ${model.fullTableName} WHERE email = $${index++} AND status <> $${index++}) AS email)`);
        args.push(model.email, pgHelpers.STATUS.DELETED);
      } else {
        ignoreEmail = true;
      }

      if (model.hasOwnProperty('username') && model.username) {
        select.push(`(SELECT EXISTS (SELECT 1 FROM ${model.fullTableName} WHERE username = $${index++} AND status <> $${index++}) AS username)`);
        args.push(model.username, pgHelpers.STATUS.DELETED);
      } else {
        ignoreUsername = true;
      }

      if (model.hasOwnProperty('phoneNumber') && model.phoneNumber) {

        select.push(`(SELECT EXISTS (SELECT 1 FROM ${model.fullTableName} WHERE ((phone_number = $${index++} OR (phone_code || substring(phone_number from 2)) = $${index++}) OR (phone_number = $${index++} OR (phone_code || substring(phone_number from 2)) = $${index++})) AND status <> $${index++}) AS phone)`);
        args.push(localePhoneNumber, localePhoneNumber, interPhoneNumber, interPhoneNumber, pgHelpers.STATUS.DELETED);

      } else {
        ignorePhone = true;
      }

      let sql = 'SELECT ' + select.join(', ');

      self.log.debug('SELECT SQL', sql, 'Args', args);

      return self.query(sql, args, opts).then(result => {
        let errors = [],
          code = '999';

        if (!ignoreEmail && result.rows[0].email) {

          code = '110';
          errors.push({
            code: code,
            message: errorHelper.getMessage(code),
            uiMessage: errorHelper.getUiMessage(code),
            source: 'email',
            constraint: 'unique'
          });
        }

        if (!ignoreUsername && result.rows[0].username) {
          code = '113';
          errors.push({
            code: code,
            message: errorHelper.getMessage(code),
            uiMessage: errorHelper.getUiMessage(code),
            source: 'username',
            constraint: 'unique'
          });
        }

        if (!ignorePhone && result.rows[0].phone) {
          code = '111';
          errors.push({
            code: code,
            message: errorHelper.getMessage(code),
            uiMessage: errorHelper.getUiMessage(code),
            source: 'phoneNumber',
            constraint: 'unique'
          });
        }

        if (errors.length === 0) {
          return resolve();
        } else {
          return reject(errors);
        }
      });
    });

  }

  /**
   * Checks if username is existed
   *
   * @param  {string}   username Username
   * @param  {Function} callback callback result function
   *
   */
  isUsernameExisted(username, callback) {

    var self = this,
      model = new this.modelClass();

    if (!username) {
      var message = 'Get ' + model.fullTableName + ' error: Input username value is null';
      self.log.warn(message);

      var err = errorHelper.emptyError({
        message: message,
        source: 'username'
      });

      return callback(err);
    }

    self.connectPool((client, done) => {


      var sql = 'SELECT EXISTS (SELECT 1 FROM ' + model.fullTableName + ' WHERE username = $1);';
      var query = client.query(sql, [username]);
      query.on('error', function(err) {
        var message = 'Checks existed ' + model.fullTableName + ' with username `' + username + '` error';
        self.log.error(message, err);
        callback(err);
      });

      query.on('row', function(row, result) {
        result.addRow(row);
      });

      query.on('end', function(result) {
        done();

        var isExisted = result.rows[0];
        if (isExisted) {
          self.log.info('The record is existed');
        } else {
          self.log.info('The record is not existed');
        }
        callback(null, isExisted);
      });

    });
  }

  /**
   * Checks if email is existed
   *
   * @param  {string}   email Email
   * @param  {Function} callback Callback result function
   *
   */
  isEmailExisted(email, callback) {

    var self = this,
      model = new this.modelClass();

    self.connectPool((client, done) => {

      if (!email) {
        var message = 'Get ' + model.fullTableName + ' error: Input email value is null';
        self.log.warn(message);

        var err = errorHelper.emptyError({
          message: message,
          source: 'email'
        });

        return callback(err);
      }

      var sql = 'SELECT EXISTS (SELECT 1 FROM ' + model.fullTableName + ' WHERE email = $1);';
      var query = client.query(sql, [email]);
      query.on('error', function(err) {
        var message = 'Checks existed ' + model.fullTableName + ' with email `' + email + '` error';
        self.log.error(message, err);
        callback(err);
      });

      query.on('row', function(row, result) {
        result.addRow(row);
      });

      query.on('end', function(result) {
        done();

        var isExisted = result.rows[0];
        if (isExisted) {
          self.log.info('The record is existed');
        } else {
          self.log.info('The record is not existed');
        }
        callback(null, isExisted);
      });

    });
  }

  /**
   * Checks if phone is existed
   *
   * @param  {string}   phone Email
   * @param  {Function} callback Callback result function
   */
  isPhoneExisted(phone, callback) {

    var self = this,
      model = new this.modelClass();

    self.connectPool(function(client, done) {

      if (!phone) {
        var message = 'Get ' + model.fullTableName + ' error: Input phone value is null';
        self.log.warn(message);

        var err = errorHelper.emptyError({
          message: message,
          source: 'phone'
        });

        return callback(err);
      }

      var sql = 'SELECT EXISTS (SELECT 1 FROM ' + model.fullTableName + ' WHERE phone = $1);';
      var query = client.query(sql, [phone]);
      query.on('error', function(err) {
        var message = 'Checks existed user with phone `' + phone + '` error';
        self.log.error(message, err);
        callback(err);
      });

      query.on('row', function(row, result) {
        result.addRow(row);
      });

      query.on('end', function(result) {
        done();

        var isExisted = result.rows[0];
        if (isExisted) {
          self.log.info('The record is existed');
        } else {
          self.log.info('The record is not existed');
        }
        callback(null, isExisted);
      });

    });
  }


  /**
   * Query single row from table, filtered by email
   *
   * @param  {string} username Username
   * @param  {Function} callback Call back function
   *
   * @return {object}   Result model
   */
  getByUsername(username, opts) {

    opts = opts || {};
    opts.source = 'username';
    let self = this,
      model = new this.modelClass();

    self.log.info('Begins getting ' + model.fullTableName + ' by username...');

    return self.checkEmpty({
      value: username,
      message: 'Get ' + model.fullTableName + ' error: Input username is null',
      source: 'username'
    }, opts).then(() => {
      username = username.toLowerCase();

      return self.getOne({
        where: 'LOWER(username) = $1',
        args: [username]
      }, opts);

    });

  }

  /**
   * Query single row from table, filtered by email
   *
   * @param  {string} email User email
   * @param  {Function} callback Call back function
   *
   * @return {object}   Result model
   */
  getByEmail(email, opts) {

    opts = opts || {};
    opts.source = 'email';
    let self = this,
      model = new this.modelClass();

    self.log.info('Begins getting ' + model.fullTableName + ' by email...');

    return self.checkEmpty({
      value: email,
      message: 'Get ' + model.fullTableName + ' error: Input email is null',
      source: 'email'
    }, opts).then(() => {
      email = email.toLowerCase();

      return self.getOne({
        where: 'LOWER(email) = $1',
        args: [email]
      }, opts);

    });
  }

  /**
   * Query single row from table, filtered by phone
   *
   * @param  {string} phone User phone
   * @param  {string} localePhone Locale phone
   * @param  {Function} callback Call back function
   *
   * @return {object}   Result model
   */
  getByPhone(phone, opts) {

    opts = opts || {};
    opts.source = 'email';

    let self = this,
      model = new this.modelClass();

    return self.checkEmpty({
      value: phone,
      message: 'Get ' + model.fullTableName + ' error: Input phone is null',
      source: 'phone'
    }, opts).then(() => {
      return self.getOne({
        where: "phone_number = $1 OR (phone_code || substring(phone_number from 2)) = $2 OR ('0' || substring(phone_number from 1 for char_length(phone_code)) = $3)",
        args: [phone, phone, phone]
      }, opts);
    });

  }

  /**
   * Query single row from table, filtered by NexID
   *
   * @param  {string} nexid NexID
   * @param  {string} opts Option data
   *
   * @return {object}   Result model
   */
  getOneByNexId(nexid, opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass();

    return self.checkEmpty({
      value: nexid,
      message: 'Get ' + model.fullTableName + ' error: Input NexID is null',
      source: 'nexid'
    }, opts).then(() => {
      return self.getOne({
        where: ['nexid = $1', 'status <> $2'],
        args: [nexid, pgHelpers.STATUS.DELETED]
      }, opts);

    });

  }

  /**
   * Query single row from table, filtered by NexID, with relationship
   *
   * @param  {string} nexid NexID
   * @param  {string} opts Option data
   *
   * @return {object}   Result model
   */
  getOneRelationByNexId(nexid, opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass();

    return self.checkEmpty({
      value: nexid,
      message: 'Get ' + model.fullTableName + ' error: Input NexID is null',
      source: 'nexid'
    }, opts).then(() => {
      return self.getOneRelation({
        where: [`${model.tableAlias}.nexid = $1`, `${model.tableAlias}.status <> $2`],
        args: [nexid, pgHelpers.STATUS.DELETED]
      }, opts);

    });

  }

  /**
   * Updates avatar data
   *
   * @param  {String}   uid       Primary key data
   * @param  {String}   avatar   Avatar json data
   * @param  {Object}   opts     Option data
   * @param  {Function} callback Callback function
   */
  updateAvatar(uid, avatar, opts) {

    opts = opts || {};

    let self = this,
      model = new this.modelClass();

    self.log.debug('Begins update ' + model.fullTableName + ' avatar...');

    return self.checkEmpty([{
      value: uid,
      message: 'updateAvatar ' + model.fullTableName + ' error: Input uid is null',
      source: 'uid'
    }, {
      value: avatar,
      message: 'updateAvatar ' + model.fullTableName + ' error: Input avatar is null',
      source: 'avatar'
    }], opts).then(() => {
      let sql = 'UPDATE ' + model.fullTableName + ' SET avatar = $1, updated_at = NOW() WHERE uid = $2',
        args = [avatar, uid];

      return self.query(sql, args, opts).then(results => {
        self.log.debug(results.rowCount + ' rows were updated');

        return BPromise.resolve(results.rowCount);
      });

    });

  }

  /**
   * Update metadata into database
   *
   * @param  {object} model Input model
   * @param  {Function} callback Call back function
   *
   * @return {mixed} true if successful insertion, error string if failed insertion
   */
  updateMetadata(uid, metadata, opts) {

    let self = this,
      model = new self.modelClass(),
      tableName = model.fullTableName;

    opts = opts || {};

    return self.checkEmpty([{
      value: uid,
      message: 'updateMetadata ' + model.fullTableName + ' error: Input uid is null',
      source: 'uid'
    }], opts).then(() => {

      return self.getOne(uid, opts).then(oldModel => {
        let oldMetadata = oldModel.metadata || {};

        if (typeof(metadata) === 'string') {
          metadata = JSON.parse(metadata);
        }

        let newMetadata = Hoek.merge(oldMetadata, metadata, false, false);
        oldModel.metadata = JSON.stringify(newMetadata);

        let sql = 'UPDATE ' + tableName + ' SET metadata=$1, updated_at=NOW() WHERE uid=$2',
          args = [oldModel.metadata, oldModel.uid];

        return self.query(sql, args, opts).then(results => {
          return BPromise.resolve(results.rowCount);
        });
      });
    });
  }

  /**
   * Update verification code into profile
   *
   * @param  {object} model Input model
   * @param  {Object} opts Option data
   *
   * @return {mixed} true if successful insertion, error string if failed insertion
   */
  updateVerificationCode(model, opts) {

    Hoek.assert(model, 'Verification form is empty, cannot update verification code');
    Hoek.assert(model.uid, 'User ID is empty, cannot update verification code');

    let self = this,
      modelInstance = new self.modelClass(),
      tableName = modelInstance.fullTableName,
      setFields = [],
      args = [];

    if (model.verificationCode) {
      setFields.push('verification_code=$' + (args.length + 1));
      args.push(model.verificationCode);
    }

    if (model.verificationExpiry) {
      setFields.push('verification_expiry=$' + (args.length + 1));
      args.push(model.verificationExpiry);
    }

    if (model.updatedBy) {
      setFields.push('updated_by=$' + (args.length + 1));
      args.push(model.updatedBy);
    }

    if (model.isVerified !== null && model.isVerified !== undefined) {
      setFields.push('is_verified=$' + (args.length + 1));
      args.push(model.isVerified);
    }

    setFields.push('updated_at = NOW()');

    let sql = 'UPDATE ' + tableName + ' SET ' + setFields.join(', ') + ' WHERE uid = $' + (args.length + 1);
    args.push(model.uid);

    return self.query(sql, args, opts).then(results => {

      return BPromise.resolve(results.rowCount);
    });
  }

  /**
   * Updates notification token to user
   *
   * @param  {String} userId User ID
   * @param  {String} token  Notification token
   *
   * @return {Integer} Updated count
   */
  updateNotificationToken(userId, token) {

    Hoek.assert(userId, 'User ID is empty. Cannot update notification token');
    Hoek.assert(token, 'Notification Token is empty. Cannot update notification token');

    let self = this,
      modelInstance = new self.modelClass(),
      tableName = modelInstance.fullTableName;

    let sql = 'UPDATE ' + tableName + ' SET notification_token = $1 WHERE uid = $2',
      args = [token, userId];

    return self.query(sql, args).then(results => {
      return BPromise.resolve(results.rowCount);
    });
  }
}

module.exports = ProfileAdapter;
