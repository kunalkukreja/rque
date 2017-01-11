var EventEmitter = require('events').EventEmitter;
var config = require('../config');
var Producer = require('./producer');
var domain = require('domain');
var util = require('util');
var host = require('../../lib/host');
var rqUtil = require('./util');

var activeExchange = new Producer(config.EXCHANGES.ACTIVE);
var failedExchange = new Producer(config.EXCHANGES.FAILED);
var delayedExchange = new Producer(config.EXCHANGES.DELAYED);

function Job(id, type, data, options) {
  this.type = type;
  this.data = data || {};
  this.id = id || host.generate_unique_id();
  this.created_at = new Date(options.created_at || new Date());
  this.published = false;
  if (options.failed_at) this.failed_at = new Date(options.failed_at);
  this.priority = options.priority || 0;
  this.attempts = options.attempts || 0;
  this.max_attempts = options.max_attempts || 3;
  this.saveOnFailure = options.saveOnFailure || false;
  // exchange options
  // Always recreate queue attribute
  this.queue = {};
  this.queue.msgOpts = {};
  this.queue.msgOpts.priority = this.priority;
  this.queue.msgOpts.headers = {};
  this.queue.key = 'jobs.' + type;
  this.failedKey = options.failedKey || null;
  this.queue.exchange = activeExchange;
  this.origin_type = options.origin_type || null;
  this.extra = options.extra || null;

  if (options.error)
    this.error = JSON.stringify(options.error);

  if (options.delay && parseInt(options.delay))
    this.delayJob(options.delay);

}

Job.prototype.save = function() {
  if (this.published == true) return;
  this.queue.exchange.publish(this.serialize(), this.queue.msgOpts, this.queue.key);
  this.published = true;
};

Job.fromString = function(jobStr) {
  var obj = JSON.parse(jobStr);
  var type = obj.type;
  var data = obj.data;
  var id = obj.id;
  delete obj.data;
  delete obj.type;
  return new Job(id, type, data, obj);
};

Job.prototype.failed = function(err) {
  this.failed_at = new Date();
  this.error = err;
  if (++this.attempts < this.max_attempts) {
    util.log(util.format('Process id:%s. Error in processing job id:%s. Error:%s. Attempts:%s',process.pid, this.id, JSON.stringify(err),this.attempts));
    this.delayJob((Math.pow(this.attempts, 2)) * 60 *1000);
    this.save();
  }
  else {
    if(this.saveOnFailure) {
      this.failJob();
      this.save();
    }
    util.log(util.format('Process id:%s. Job failed after maximum attempts with id: %s. Error: %s. Attempts:%s',process.pid, this.id, JSON.stringify(err),this.attempts));
  }
};

Job.prototype.failJob = function () {
    console.log("Queueing failed job:",this.type, this.id);
    if(this.failedKey) this.queue.key = 'jobs.' + this.failedKey;
    this.queue.exchange = failedExchange;
};

Job.prototype.delayJob = function (delay_in_sec) {
  this.queue.exchange = delayedExchange;
  this.queue.key = rqUtil.delayKey(delay_in_sec);
  util.log(util.format('Pushing jobs to delayed exchange. job_id: %s, type: %s, delay: %s, routing key: %s', this.id, this.type, delay_in_sec, this.queue.key));
  this.origin_type = this.type;
  this.type = 'delay';
};

/**
 * Inherit from `EventEmitter.prototype`.
 */

Job.prototype.__proto__ = EventEmitter.prototype;

/**
 * Return JSON-friendly object.
 *
 * @return {Object}
 * @api public
 */

Job.prototype.toJSON = function() {
  return {
    id: this.id, type: this.type, data: this.data, extra: this.extra, result: this.result, priority: this.priority, state: this.state, error: this.error, created_at: this.created_at, updated_at: this.updated_at, failed_at: this.failed_at, error: this.error, duration: this.duration, delay: this.delay, promote_at: this.promote_at, attempts: {
      made: Number(this.attempts), remaining: this.attempts ? this.max_attempts - this.attempts : Number(this.max_attempts) || 1, max: Number(this.max_attempts) || 1}, saveOnFailure: this.saveOnFailure, failedKey: this.failedKey, max_attempts:this.max_attempts,origin_type:this.origin_type
  };
};

Job.prototype.serialize = function() {
  var obj = {
    id: this.id, type: this.type, data: this.data, extra: this.extra, error: this.error, created_at: this.created_at, updated_at: this.updated_at, failed_at: this.failed_at, duration: this.duration, delay: this.delay, promote_at: this.promote_at, attempts: this.attempts,
    priority: this.priority, saveOnFailure: this.saveOnFailure, failedKey: this.failedKey, max_attempts:this.max_attempts,origin_type:this.origin_type
  };
  return JSON.stringify(obj);
};

// Takes task handler and callback function
Job.prototype.process = function(fn, done) {
  var that = this;

  this.started_at = new Date();

  // Create a domain for each job to handle uncaughtException
  var d = domain.create();

  // Catch async error via domain
  d.on('error', function(err) {
    err = err.stack.split('\n');
    done(err);
  });

  // execute taskHandler in domain
  d.run(function() {
    // catch sync errors
    try {
      fn(that, done);
    } catch (e) {
      done(e);
    }
  });
};

module.exports = Job;


/************** TEST Code *****************/
(function() {
  if (require.main === module) {

    var j = new Job(null,'notify1', { mail: { subject: 'Your Order has been successfully placed',
      text:'hello there',
      from: 'No Reply <no-reply@zyx.com>',
      to: [ 'kukrejakunal@yahoo.com' ],
      cc: [],
      bcc: '',
      html: 'Hello World',
      headers: { 'X-SMTPAPI': '{"sub":{"%ORDERID%":["1368530295"]},"category":["M:P","V:4","S:7","order_delivered"]}' } },
      mailOptions: { to: [ 'random.kumar@xyz.com' ],
        cc: [],
        async: true,
        logData: { job_id: '214306688',
          user_id: 15016267,
          type: 'email',
          resource_type: 'Order',
          resource_id: 1368530295,
          resource_state: 7,
          item_ids: [1468530295, 1468530296],
          merchant_id: 2,
          template_id: 3 },
        subject: 'Your Order has been successfully placed',
        body: 'hi',
        bcc: '',
        validationError: [] } }, {});
    j.save();

  }
}());
