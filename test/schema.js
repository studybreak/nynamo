var async = require('async');
var assert = require('assert');
var dynamo = require("dynamo");
var should = require('should');

var nyanmo = require('../lib/server.js');


var port = 1337;

var client, server, db;

describe('dynamo', function () {

  before(function (done) {
    client = dynamo.createClient({
      accessKeyId: 'AKIAIUUVZ77SMBWSZYMA',
      secretAccessKey: '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'
    });

    db = client.get('us-west-1');
    db.host = 'localhost';
    dynamo.Request.prototype.port = port;

    server = nyanmo.run(port, done);
  });

  it('should create a table', function (done) {
    db.add({
      name: "test",
      throughput: {read: 10, write: 10}
    }).save(done);
  });

  it('should create another table', function (done) {
    db.add({name: "foobar"}).save(done);
  });

  it('should try to create an invalid table', function (done) {
    db.add({name: "*++Table"}).save(function (err, table) {
      should.exist(err);
      err.should.have.property('statusCode', 400);
      done();
    });
  });

  it('should list tables', function (done) {
    db.fetch(function(err) {
      if (err) return done(err);
      db.tables.should.have.keys('test', 'foobar');
      done();
    });
  });

  it('should delete a table', function (done) {
    db.remove('foobar', done);
  });

  it('should describe a table', function (done) {
    db.get('test').fetch(function (err, table) {
      if (err) return done(err);
      table.should.have.keys('CreationDateTime', 'KeySchema',
          'ProvisionedThroughput', 'TableName', 'TableStatus');
      table.should.have.property('TableName', 'test');
      table.ProvisionedThroughput.should.have.property('ReadCapacityUnits', 10);
      table.ProvisionedThroughput.should.have.property('WriteCapacityUnits', 10);
      done();
    });
  });

  it('should put data', function (done) {
    db.put('test', {id: '1', name: 'Allan'}).save(done);
  });

  it('should get data', function (done) {
    db.get('test', {id: '1'}).fetch(function (err, result) {
      result.should.have.keys(['id', 'name']);
      result.should.have.property('id', '1');
      result.should.have.property('name');
      done();
    });
  });

  it('should delete data', function (done) {
    db.deleteItem({
      TableName: 'test',
      Key: {HashKeyElement:{S:'1'}},
      ReturnValues: 'NONE'
    }, done);
  });

  it('should delete data and return it', function (done) {
    db.deleteItem({
      TableName: 'test',
      Key: {HashKeyElement:{S:'1'}},
      ReturnValues: 'ALL_OLD'
    }, done);
  });

  it('should update a table', function (done) {
    db.updateTable({
      TableName: 'test',
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 2
      }
    }, function (err, data) {
      if (err) return done(err);
      var table = data.TableDescription;
      table.should.have.keys('CreationDateTime', 'ItemCount', 'KeySchema',
          'ProvisionedThroughput', 'TableName', 'TableStatus', 'TableSizeBytes');
      table.ProvisionedThroughput.should.have.property('ReadCapacityUnits', 1);
      table.ProvisionedThroughput.should.have.property('WriteCapacityUnits', 2);
      done();
    });
  });

  it('should respect put capacity limits', function (done) {
    this.timeout(3000);

    setTimeout(function () {
      async.map(['1', '2', '3'], function (id, callback) {
        db.put('test', {id: id, name: 'Allan'}).save(function (err) {
          return callback(null, err);
        });
      }, function (err, results) {
        results.filter(function (err) { return err; }).should.have.length(1);
      });
    }, 1000);

    setTimeout(function () {
      db.put('test', {id: '1', name: 'Allan'}).save(function (err) {
        should.not.exist(err);
        done();
      });
    }, 2000);
  });

  it('should respect get capacity limits', function (done) {
    this.timeout(3000);

    setTimeout(function () {
      async.map(['1', '1', '1'], function (id, callback) {
        db.get('test', {id: id}).fetch(function (err) {
          return callback(null, err);
        });
      }, function (err, results) {
        results.filter(function (err) { return err; }).should.have.length(1);
      });
    }, 1000);

    setTimeout(function () {
      db.get('test', {id: '1'}).fetch(function (err) {
        should.not.exist(err);
        done();
      });
    }, 2000);
  });


});


function repeat(pattern, count) {
    if (count < 1) return '';
    var result = '';
    while (count > 0) {
        if (count & 1) result += pattern;
        count >>= 1, pattern += pattern;
    }
    return result;
}
