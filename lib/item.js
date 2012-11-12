
var Item = module.exports = function Item(item) {

};

Item.size = function (items) {
  items = Array.isArray(items) ? items : [items];
  var size = 0, item;
  for (var j = items.length - 1; j >= 0; j--) {
    item = items[j];
    for (var i = 0, keys = Object.keys(item), len = keys.length; i < len; i++) {
      size += utf8Length(keys[i].toString());
      size += utf8Length(item[keys[i]].toString());
    }
  }
  return size;
};

Item.prototype.size = function () { return Item.size(this); };


Item.parseItem = function (schema, item) {
  if (!item) return new Error('Invalid item');

  console.log('Parsing Item: ');
  console.log(item);
  console.log();

  var parsed = {};
  for (var i = 0, keys = Object.keys(item), len = keys.length; i < len; i++) {
    var name = keys[i];
    var attr = item[name];
    if (!attr) return new Error('Empty key found: ' + name);

    var value = Item.parseAttribute(schema, name, attr);
    if (value instanceof Error) return value;

    parsed[keys[i]] = value[2];
  }

  if (Item.size(parsed) > 64*1024)
    return new Error('Item exceeds 64k size limit: ' + parsed);

  console.log('Parsed Item: ');
  console.log(parsed);
  console.log();

  return parsed;
};


Item.parseAttribute = function (schema, name, attr) {
  var type = Object.keys(attr)[0];
  var value = attr[type];
  var parsed;

  var isHashKey = schema.HashKeyElement.AttributeName === name;
  if (isHashKey && schema.HashKeyElement.AttributeType !== type)
    return new Error('Item does not match schema at hash key: ' + name);

  var isRangeKey = schema.RangeKeyElement &&
      schema.RangeKeyElement.AttributeName === name;
  if (isRangeKey && schema.RangeKeyElement.AttributeType !== type)
    return new Error('Item does not match schema at range key: ' + name);

  if (type === 'N') {
    parsed = parseInt(value, 10);
    if (isNaN(parsed)) return new Error('Invalid number: ' + attr);
  }
  else if (type === 'S') {
    parsed = value;
  }
  else if (type === 'B') {
    parsed = item.base64Decode(value);
  }
  else {
    return new Error('Unrecognized attribute data type: ' + type);
  }

  return [type, value, parsed];
};


Item.parseKey = function (schema, key) {
  if (!key) return new Error('Invalid key');

  var result = {};
  var name, attr, parsed;

  if (!key.HashKeyElement)
    return new Error('HashKeyElement not found while parsing key');

  name = schema.HashKeyElement.AttributeName;
  attr = key.HashKeyElement;
  parsed = Item.parseAttribute(schema, name, attr);
  if (parsed instanceof Error) return parsed;
  result[name] = parsed[2];

  if (key.RangeKeyElement) {
    name = schema.RangeKeyElement.AttributeName;
    attr = key.RangeKeyElement;
    parsed = Item.parseAttribute(schema, name, attr);
    if (parsed instanceof Error) return parsed;
    result[name] = parsed[2];
  }

  return result;
};


var base64Decode = Item.base64Decode = function base64Decode (input) {
    return new Buffer(input || '', 'base64');
};


var base64Encode = Item.base64Encode = function base64Encode (input) {
    return (new Buffer(input, 'utf8')).toString('base64') || '';
};


var utf8Length = Item.utf8Length = function utf8Length (string) {
  var utf8length = 0;
  for (var n = 0; n < string.length; n++) {
    var c = string.charCodeAt(n);
    if (c < 128) {
      utf8length++;
    }
    else if((c > 127) && (c < 2048)) {
      utf8length = utf8length+2;
    }
    else {
      utf8length = utf8length+3;
    }
  }
  return utf8length;
};



function makeError(code, message, type, description) {
  var err = new Error(message);
  err.code = code;
  err.type = type;
  err.description = description;
  return err;
}
