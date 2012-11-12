
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



function makeError(code, message, type, description) {
  var err = new Error(message);
  err.code = code;
  err.type = type;
  err.description = description;
  return err;
}
