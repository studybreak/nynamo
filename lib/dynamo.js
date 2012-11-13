var Item = require('./item');
var Table = require('./table');


var Dynamo = module.exports = function Dynamo (options) {
  this.schema = {};
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

  callback(null, {TableDescription: table.getDescription(false)});
};


Dynamo.prototype.DeleteTable = function (data, callback) {
  var table = this.getTable(data.TableName);
  if (table instanceof Error) return callback(table);

  if (table.TableStatus !== 'ACTIVE')
    return makeError(400, '', 'ResourceInUseException', 'Table is ' + table.TableStatus);

  table.deleteTable();
  delete this.schema[data.TableName];

  callback(null, {TableDescription: table.getDescription(false)});
};


Dynamo.prototype.DescribeTable = function (data, callback) {
  var table = this.getTable(data.TableName);
  if (table instanceof Error) return callback(table);

  callback(null, {TableDescription: table.getDescription(true)});
};


Dynamo.prototype.GetItem = function (data, callback) {
  var table = this.getTable(data.TableName);
  if (table instanceof Error) return callback(table);

  table.getItem(data.Key, function (err, item, capacity) {
    if (err) return callback(err);
    callback(null, {
      Item: item,
      ConsumedCapacityUnits: capacity
    });
  });
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

  if (data.ReturnValues && data.ReturnValues !== 'NONE' && data.ReturnValues !== 'ALL_OLD') {
    return callback(new Error(400, '', 'ValidationException', 'Invalid ReturnValues'));
  }

  table.putItem(data.Item, function (err, old, capacity) {
    if (err) return callback(err);

    var result = {
      Attributes: data.ReturnValues === 'ALL_OLD' ? old : undefined,
      ConsumedCapacityUnits: capacity
    };

    callback(null, result);
  });
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
