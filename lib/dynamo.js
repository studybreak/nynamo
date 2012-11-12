var Item = require('./item');
var Table = require('./table');


var Dynamo = module.exports = function Dynamo (options) {
  this.schema = {};
  this.contents = {};
};



Dynamo.validateKey = function (schema, key) {
  return true;
};


Dynamo.makeKey = function (schema, item) {
  return item[schema.HashKeyElement.AttributeName] + '$' +
         (schema.RangeKeyElement && item[schema.RangeKeyElement.AttributeName] || '');
};

Dynamo.capacity = function (items) {
  return Math.ceil(Item.size(items) / 1024);
};

Dynamo.prototype.getTable = function (name) {
  if (!name)
    return makeError(400, '', 'ValidationException', 'Missing table name');

  var table = this.schema[name];

  if (!table)
    return makeError(400, '', 'ResourceNotFoundException', 'Unknown table');

  return table;
};


Dynamo.prototype.CreateTable = function (data, callback) {
  var table = new Table(data);
  if (table instanceof Error) return callback(table);

  this.schema[table.TableName] = table;
  this.contents[table.TableName] = {};

  callback(null, {TableDescription: table.getDescription(false)});
};


Dynamo.prototype.DeleteTable = function (data, callback) {
  var table = this.getTable(data.TableName);
  if (table instanceof Error) return callback(table);

  if (table.TableStatus !== 'ACTIVE')
    return makeError(400, '', 'ResourceInUseException', 'Table is ' + table.TableStatus);

  table.deleteTable();

  delete this.schema[data.TableName];
  delete this.contents[data.TableName];

  callback(null, {TableDescription: table.getDescription(false)});
};


Dynamo.prototype.DescribeTable = function (data, callback) {
  var table = this.getTable(data.TableName);
  if (table instanceof Error) return callback(table);

  callback(null, {TableDescription: table.getDescription(true)});
};


Dynamo.prototype.GetItem = function (data, callback) {
  var table = this.schema[data.TableName];

  if (!table)
    return callback(makeError(400, '', 'ResourceNotFoundException', 'Unknown table'));

  if (!Dynamo.validateKey(table.KeySchema, data.Key))
    return callback(makeError(400, '', 'ValidationException', 'Invalid key'));

  var key = Item.parseKey(table.KeySchema, data.Key);
  if (key instanceof Error)
    return callback(makeError(400, '', 'ValidationException', key.message));

  key = Dynamo.makeKey(table.KeySchema, key);

  var item = this.contents[table.TableName][key];
  var result = {
    Item: item,
    ConsumedCapacityUnits: Dynamo.capacity(item)
  };

  callback(null, result);
};


Dynamo.prototype.ListTables = function (data, callback) {
  var names = Object.keys(this.schema).sort();
  var limit = data.limit && parseInt(data.limit, 10) || names.length;

  if (data.limit !== undefined && isNaN(limit))
    return callback(makeError(400, '', 'ValidationException', 'Invalid limit'));

  callback(null, {
    TableNames: names.slice(0, limit),
    LastEvaluatedTableName: limit < names.length ? names[limit] : undefined
  });
};


Dynamo.prototype.PutItem = function (data, callback) {
  var table = this.getTable(data.TableName);
  if (table instanceof Error) return callback(table);

  var item = Item.parseItem(table.KeySchema, data.Item);
  if (item instanceof Error)
    return callback(makeError(400, '', 'ValidationException', item.message));

  var key = Dynamo.makeKey(table.KeySchema, item);

  // Store the unparsed item so we remember its attribute types.
  var old = this.contents[table.TableName][key];
  this.contents[table.TableName][key] = data.Item;

  var result = {};

  if (data.ReturnValues === 'ALL_OLD') {
    result.Attributes = old;
  }
  else if (data.ReturnValues && data.ReturnValues !== 'NONE') {
    return callback(new Error(400, '', 'ValidationException', 'Invalid ReturnValues'));
  }

  result.ConsumedCapacityUnits =
      Math.max(Dynamo.capacity(item), old && Dynamo.capacity(old) || 0);

  callback(null, result);
};


Dynamo.prototype.UpdateTable = function (data, callback) {
  var table = this.getTable(data.TableName);
  if (table instanceof Error) return callback(table);

  table.updateProvisioning(data.ProvisionedThroughput);

  callback(null, {TableDescription: table.getDescription(true)});
};



function makeError(code, message, type, description) {
  var err = new Error(message);
  err.code = code;
  err.type = type;
  err.description = description;
  return err;
}
