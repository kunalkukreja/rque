"use strict";


var RQ = require('rqueue');
var rqConfig = require('../../config');
var rq = new RQ(rqConfig.rq);
var util = require('util');

var delay_interval = [1, 4, 9, 16, 25];
var delay_key = 'jobs_delay.';

var myUtil = {
  getDelayQueues: function (exchange) {
    var commonFields = {
      PREFETCH: 4,
      OPTIONS: {
        exclusive: false,
        durable: true,
        autoDelete: false,
        arguments: {
          "x-dead-letter-exchange": "ex_jobs_active"
        }
      },
      CONSUMEOPTS: {
        noAck: false
      },
      EXCHANGE_OPTS: exchange
    };
    var queues = [];
    for (var d in delay_interval) {
      var queue = JSON.parse(JSON.stringify(commonFields));
      queue.NAME = 'q_delay_' + delay_interval[d] + '_mins';
      queue.OPTIONS.arguments['x-message-ttl'] = delay_interval[d] * 60000;
      queue.BINDINGKEY = delay_key + delay_interval[d];
      queues.push(queue);
    }
    return queues;

  },

  delayKey: function (delay_in_sec) {
    var delay_mins = (parseInt(delay_in_sec) / 60000).toFixed();
    if (delay_mins <= delay_interval[0]) return delay_key + delay_interval[0];
    for (var d = 1; d < delay_interval.length; d++) {
      if (delay_interval[d] >= delay_mins) {
        if (Math.abs(delay_interval[d] - delay_mins) < Math.abs(delay_interval[d - 1] - delay_mins))
          return delay_key + delay_interval[d];
        return delay_key + delay_interval[d - 1];
      }
    }
    return delay_key + delay_interval[delay_interval.length - 1];
  },

  initQueues: function (cb) {
    var config = require('../config');
    var queues = Object.keys(config.QUEUES);
    var count = queues.length;
    queues.forEach(function (queue) {
      rq.oneTimeTask__assertAndBind(config.QUEUES[queue].EXCHANGE_OPTS, config.QUEUES[queue], function (e, t) {
        if (e) console.log(e);
        if (--count == 0) {
          console.log("queues successfully created");
          return cb();
        }
      });
    });
  }
};
module.exports = myUtil;
