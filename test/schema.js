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
    db.add({name: "test"}).save(function (err, table) {
      done(err);
    });
  });

  it('should create another table', function (done) {
    db.add({name: "foobar"}).save(function (err, table) {
      done(err);
    });
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
      table.ProvisionedThroughput.should.have.property('ReadCapacityUnits', 3);
      table.ProvisionedThroughput.should.have.property('WriteCapacityUnits', 5);
      done();
    });
  });

  it('should update a table', function (done) {
    db.updateTable({
      TableName: 'test',
      ProvisionedThroughput: {
        ReadCapacityUnits: 10,
        WriteCapacityUnits: 10
      }
    }, function (err, data) {
      if (err) return done(err);
      var table = data.TableDescription;
      table.should.have.keys('CreationDateTime', 'ItemCount', 'KeySchema',
          'ProvisionedThroughput', 'TableName', 'TableStatus', 'TableSizeBytes');
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
      result.should.have.property('name', 'Allan');
      done();
    });
  });

});
