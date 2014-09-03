var influx = require('influx');
var mysql = require('mysql');
var config = require('../secrets').config;
var chai = require('chai');

var icon = influx(config.influx);
var gcon = mysql.createConnection(config.gratia.url);

describe('influxdb', function() {
    it('should post data to influx', function(done) {
        icon.writePoint('test', {value: 'garbage'}, function(err) {
            if(err) throw err;
            done();
        });
    });  
    it('should query data from influx', function(done) {
        icon.query('SELECT value FROM test WHERE time > now() - 24h', function(err, ret) {
            if(err) throw err;
            chai.expect(ret[0].points).to.be.not.empty;
            done();
        });
    });  
});
