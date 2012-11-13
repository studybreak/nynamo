var Item = require('./item');


var properties = ['CreationDateTime', 'ItemCount', 'KeySchema',
                  'ProvisionedThroughput', 'TableName', 'TableStatus',
                  'TableSizeBytes'];


var Table = module.exports = function Table (table) {
  if (!Table.validateTableName(table.TableName))
    return makeError(400, '', 'ValidationException', 'Invalid table name');

  if (!Table.validateKeySchema(table.KeySchema))
    return makeError(400, 'ValidationException', 'Invalid key schema');

  if (!Table.validateProvisioning(table.ProvisionedThroughput))
    return makeError(400, 'ValidationException', 'Invalid provisioning');

  this.CreationDateTime = Math.floor(new Date().getTime() / 1000);
  this.KeySchema = {
    HashKeyElement: table.KeySchema.HashKeyElement,
    RangeKeyElement: table.KeySchema.RangeKeyElement
  };
  this.ProvisionedThroughput = {
    ReadCapacityUnits: table.ProvisionedThroughput.ReadCapacityUnits,
    WriteCapacityUnits: table.ProvisionedThroughput.WriteCapacityUnits
  };
  this.TableName = table.TableName;

  this.ItemCount = 0;
  this.TableSizeBytes = 0;

  // Track the throughput usage using a sparse per-second list.
  this.throughput = {
    Read: [],
    Write: []
  };

  // Temporary place to store content until we move to something persistent
  this.contents = {};

  this.setStatus('CREATING');
};


Table.validateTableName = function (tableName) {
  return tableName && /^[a-zA-Z0-9_\-.]+$/.test(tableName);
};


Table.validateKeySchema = function (key) {
  return key && key.HashKeyElement &&
         typeof key.HashKeyElement.AttributeName === 'string' &&
         /^[SNB]$/.test(key.HashKeyElement.AttributeType) &&
         (!key.RangeKeyElement ||
         typeof key.RangeKeyElement.AttributeName === 'string' &&
         /^[SNB]$/.test(key.RangeKeyElement.AttributeType));
};


Table.validateKey = function (schema, key) {
  return true;
};


Table.validateProvisioning = function (provisioning) {
  return provisioning && typeof provisioning.ReadCapacityUnits === 'number' &&
      typeof provisioning.WriteCapacityUnits === 'number';
};


Table.prototype.setStatus = function (status) {
  this.TableStatus = status;

  var self = this;
  process.nextTick(function () {
    self.TableStatus = 'ACTIVE';
  });
};


Table.prototype.getDescription = function (includeSizes) {
  var result = {}, key;
  for (var i = 0, len = properties.length; i < len; i++) {
    key = properties[i];
    if (key in this) result[key] = this[key];
  }

  if (includeSizes !== undefined && !includeSizes) {
    delete result.ItemCount;
    delete result.TableSizeBytes;
  }

  return result;
};


Table.prototype.updateProvisioning = function (provisioning) {
  if (!Table.validateProvisioning(provisioning))
    return makeError(400, '', 'ValidationException', 'Invalid provisioning');

  var current = this.ProvisionedThroughput;

  var increase = current.ReadCapacityUnits > provisioning.ReadCapacityUnits ||
      current.WriteCapacityUnits > provisioning.WriteCapacityUnits;
  if (increase) {
    this.ProvisionedThroughput.LastIncreaseDateTime = new Date().getTime();
  }

  var decrease = current.ReadCapacityUnits < provisioning.ReadCapacityUnits ||
      current.WriteCapacityUnits < provisioning.WriteCapacityUnits;
  if (decrease) {
    this.ProvisionedThroughput.LastDecreaseDateTime = new Date().getTime();
  }

  this.ProvisionedThroughput.ReadCapacityUnits = provisioning.ReadCapacityUnits;
  this.ProvisionedThroughput.WriteCapacityUnits = provisioning.WriteCapacityUnits;

  this.setStatus('UPDATING');
};


Table.prototype.deleteTable = function () {
  this.setStatus('DELETING');
};


Table.prototype.makeContentKey = function (item) {
  var schema = this.KeySchema;
  return item[schema.HashKeyElement.AttributeName] + '$' +
         (schema.RangeKeyElement && item[schema.RangeKeyElement.AttributeName] || '');
};


Table.prototype.getItem = function (key, callback) {
  var parsedKey = Item.parseKey(this.KeySchema, key);
  if (parsedKey instanceof Error) return callback(parsedKey);

  var item = this.contents[this.makeContentKey(parsedKey)];

  var capacity = this.testCapacity('Read', item);
  if (capacity instanceof Error) return callback(capacity);

  callback(null, item, capacity);

  this.useCapacity('Read', capacity);
};


Table.prototype.putItem = function (item, callback) {
  var parsedItem = Item.parseItem(this.KeySchema, item);
  if (parsedItem instanceof Error)
    return callback(makeError(400, '', 'ValidationException', parsedItem.message));

  var key = this.makeContentKey(parsedItem);
  var existingItem = this.contents[key];

  var newCapacity = this.testCapacity('Write', parsedItem);
  if (newCapacity instanceof Error) return callback(newCapacity);
  var oldCapacity = this.testCapacity('Write', existingItem);
  if (oldCapacity instanceof Error) return callback(oldCapacity);


  var capacity = Math.max(newCapacity, oldCapacity);

  callback(null, existingItem, capacity);

  // Store the unparsed item so we remember its attribute types.
  this.contents[key] = item;
  this.useCapacity('Write', capacity);
};


Table.prototype.testCapacity = function (type, item) {
  var throughput = Item.capacity(item);

  var now = Math.floor(new Date().getTime() / 1000);
  var left = this.ProvisionedThroughput[type + 'CapacityUnits'] -
             (this.throughput[type][now] || 0) - throughput;

  if (left < 0) {
    var err = 'Tried to use ' + throughput + ' ' + type + ' throughput on "' +
              this.TableName + '". ' + (-left) + ' more units needed.';
    return makeError(400, '', 'ProvisionedThroughputExceededException', err);
  }

  return throughput;
};


Table.prototype.useCapacity = function (type, throughput) {
  var now = Math.floor(new Date().getTime() / 1000);
  this.throughput[type][now] = (this.throughput[type][now] || 0) + throughput;
};


function makeError(code, message, type, description) {
  var err = new Error(message);
  err.code = code;
  err.type = type;
  err.description = description;
  return err;
}
