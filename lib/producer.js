'use strict';

/**
 *
 * Wrapper on rqueue. Exposes producer functionality for background processing
 *
 **/

var RQ = require('rqueue');
var config = require('../../config');
var util = require('util');
var rq = new RQ(config.rq);

/**
 *
 * Producer constructor
 * @param {Object} options: like exchangeObj etc
 *
 **/
function Producer(exchangeObj, routingKey, opts) {
  if (!opts) opts = {};
  var self = this;

  self.publisherStarted = false;
  self.publisher = rq.getSimplePublisher(exchangeObj, routingKey);
  util.log('RQ :: Producer :: Starting with exchangeObj: ' + JSON.stringify(exchangeObj) + ', routingKey: ' + routingKey);

  self.publisher.start({}, function() {
    self.publisherStarted = true;
  });
}

/**
 *
 * Producer publish method
 * @param {Object} data: Data to pe passed
 * @param {Object} options: like routingKey etc
 *
 **/
Producer.prototype.publish = function(payload, msgOpts, key) {
  if (!msgOpts) msgOpts = {};
  var self = this;
  if (self.publisherStarted === false) {
    util.log('RQ :: Publisher :: Waiting to connect...');
    return setTimeout(self.publish.bind(self), 1000, payload,msgOpts,key);
  } else {
    self.publisher.publish(payload, msgOpts, key);
  }
};

module.exports = Producer;

// ========================= Test Code =======================

// function testHandler1(msg, cb) {
//   console.log(" [x1] %s:'%s'", msg.fields.routingKey, msg.content.toString());
//   cb();
// }

// function testHandler2(msg, cb) {
//   console.log(" [x2] %s:'%s'", msg.fields.routingKey, msg.content.toString());
//   cb();
// }

(function() {
  if (require.main === module) {
    var queueConfig = require('../config');
    var p = new Producer(queueConfig.NOTIFY.QUEUE.EXCHANGE_OPTS, queueConfig.NOTIFY.QUEUE.BINDINGKEY);
    console.log('publishing messages ');
    var j = 0;
    var iterator = function() {
      for (var i = 0; i < 1000; i++) {
        var data = '{ a: ' + (i+j) + '}';
        console.log(data);
        p.publish(data,{headers: {'x-delay': 5}});
      }
      j = j + 1000;
    };
    setInterval(iterator, 500);
    console.log("Done");
  }
}());
