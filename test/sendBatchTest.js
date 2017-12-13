var Task = require('../lib/sqstask');
var assert = require("assert");
var CONF = require('config');
var should = require('should');

var AWSConfig = {   accessKeyId:     CONF.credentials.aws.accesskey,
    secretAccessKey: CONF.credentials.aws.secret,
    awsAccountId:    CONF.credentials.aws.account,
    queuePrefix:     CONF.queue.environment
};



describe('sqs', function(){
    describe('#putBatch()', function(){
        it('should put a single item on the ios-worker queue', function(done){
            var sqs = new Task(AWSConfig);
            sqs.putBatch('ios-worker', [{'asdf':'asdf'}], function(err, data){
                if(err){
                    throw err;
                }
                done();
            });
        });
        it('should fail when messages is not an array', function(done){
            var sqs = new Task(AWSConfig);
            sqs.putBatch('ios-worker', {'asdf':'asdf'}, function(err, data){
                err.should.be.an.Error;
                done();
            });
        });
        it('should fail when messages is greater than 10', function(done){
            var sqs = new Task(AWSConfig);
            var messages = [];
            for(var i=0;i<11;i++) {
                messages.push({'asdf':'asdf'});
            }
            sqs.putBatch('ios-worker', messages, function(err, data){
                err.should.be.an.Error;
                done();
            });
        });
    })
});