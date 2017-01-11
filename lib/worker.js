'use strict';

/**
 *
 * Wrapper on rqueue. Exposes consumer functionality for background processing
 *
 **/

var RQ = require('rqueue');
var config = require('../../config');
var Job = require('./job');
var util = require('util');
var rq = new RQ(config.rq);

/**
 *
 * Worker constructor
 * @param {Function} taskHandler: handler function which consumes message. This function must be of
 *                                type Func(msg,callback)
 *
 * @param {Object} options: like exchangeObj, queue options etc
 *
 **/
function Worker(exchangeObj, queueObj, opts, taskHandler) {
  if (!opts) opts = {};
  var self = this;
  self.subscriberStarted = false;
  self.subscriber = rq.getSimpleSubscriber(exchangeObj, queueObj);
  util.log('RQ :: Worker :: Starting with exchangeObj: ' + JSON.stringify(exchangeObj) + ', queue: ' + JSON.stringify(queueObj));
  self.subscriber.on('message', function (msg) {
    var job;
    function done(err) {
      if(err && err.level && err.level === 1){ // HACK: err level 1 means reprocess the job without delaying or increasing attempts
        setTimeout(function(){ self.subscriber.nack(msg); },10 * 1000);
      }else if (err) { // reprocess the job with delay and increasing attempts
        util.log("Error in processing job: "+job.type+",id:"+ job.id+",Error:"+ err);
        job.failed(err);
        self.subscriber.ack(msg);
      } else { // no error ack the message
        util.log("[rqueue success]: "+job.type +",id:"+ job.id);
        self.subscriber.ack(msg);
      }
    }
    job = Job.fromString(msg.content.toString());
    job.process(taskHandler, done);
  });

  self.subscriber.start({}, function () {
    self.subscriberStarted = true;
  });

}

module.exports = Worker;

// ========================= Test Code =======================

// function testHandler1(msg, cb) {
//   console.log(" [x1] %s:'%s'", msg.fields.routingKey, msg.content.toString());
//   cb();
// }

// function testHandler2(msg, cb) {
//   console.log(" [x2] %s:'%s'", msg.fields.routingKey, msg.content.toString());
//   cb();
// }

// (function() {
//   if (require.main === module) {
//     var type = process.argv[2];
//     if (type === 'consumer') {
//       var testWorker = new Worker(testHandler1, {});
//       var testWorker1 = new Worker(testHandler2, {});
//     } else {
//       var p = new Producer({
//         routingKey: 'SomeKey'
//       });
//       console.log('publishing messages ');
//       for (var i = 0; i < 10; i++) {
//         p.publish('{ a: ' + i + '}');
//       }
//       console.log("Done");
//     }
//   }
// }());
