var crc = require('crc');
var http = require('http');

var Dynamo = require('./dynamo');


var dynamo = new Dynamo();


exports.run = function run (port, callback) {
  var server = http.createServer(listener);
  server.listen(port || 1337, '127.0.0.1', function (err) {
    console.log('Server running at http://127.0.0.1:1337/');
    callback(err);
  });
  return server;
};


function listener (req, res) {
  executeRequest(req, res, function (err, body) {
    sendResponse(req, res, err, body);
  });
}


function executeRequest (req, res, callback) {
  req.amzTarget = req.headers['x-amz-target'].split('.')[1];
  readBody(req, res, function (err, body) {
    if (err) callback(err);
    console.log('Request: ', req.amzTarget);
    console.dir(body);
    console.log();
    dynamo[req.amzTarget].call(dynamo, body, callback);
  });
}


function readBody (req, res, callback) {
  if (req.method !== 'POST') return callback();

  var data = '';
  req.on('data', function (chunk) {
      data += chunk;

      // DynamoDB has a 1MB request size limit
      if (data.length > 1e6) {
          req.connection.destroy();
          return callback(makeError(413, "Request Entity Too Large"));
      }
  });
  req.on('end', function () {
      try {
        var body = JSON.parse(data);
        if (callback) callback(null, body);
      } catch (e) {
        if (callback) callback(e);
      }
  });
  req.on('error', function (err) {
    if (callback) callback(err);
  });
}


var requestId = 0;
function sendResponse(req, res, err, result) {
  console.log('Response: ', req.amzTarget);
  console.dir(err ? err : result);
  if (err) console.log(err.stack);
  console.log();

  var status = err && err.code || 200;
  var message = err && err.message || "OK";
  var body = !err ? result : err.type && {
    __type: "com.amazonaws.dynamodb.v20111205#" + err.type,
    message: err.description
  } || "";

  body = JSON.stringify(body);

  res.setHeader('x-amzn-RequestId', requestId++);
  res.setHeader('content-type', 'application/x-amz-json-1.0');
  res.setHeader('x-amz-crc32', crc.crc32(body));

  res.writeHead(status);
  res.end(body);
}


function makeError(code, message, type, description) {
  var err = new Error(message);
  err.code = code;
  err.type = type;
  err.description = description;
  return err;
}
