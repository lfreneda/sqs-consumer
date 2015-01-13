var EventEmitter = require('events').EventEmitter;
var util = require('util');
var _ = require('lodash');
var async = require('async');
var AWS = require('aws-sdk');
var debug = require('debug')('sqs-consumer');
var requiredOptions = [
    'queueUrl',
    'region',
    'handleMessage'
  ];

function validate(options) {
  requiredOptions.forEach(function (option) {
    if (!options[option]) {
      throw new Error('Missing SQS consumer option [' + option + '].');
    }
  });
}

/**
 * An SQS consumer.
 * @param {object} options
 * @param {string} options.queueUrl
 * @param {string} options.region
 * @param {function} options.handleMessage
 * @param {number} options.concurrency
 * @param {object} options.sqs
 */
function Consumer(options) {
  validate(options);

  this.queueUrl = options.queueUrl;
  this.handleMessage = options.handleMessage;
  this.stopped = true;
  this.sqs = options.sqs || new AWS.SQS({
    region: options.region
  });
  this.queue = async.queue(this._processMessage.bind(this), options.concurrency || 1);
}

util.inherits(Consumer, EventEmitter);

/**
 * Start polling for messages.
 */
Consumer.prototype.start = function () {
  if (this.stopped) {
    debug('Starting consumer');
    this.stopped = false;
    this._poll();
  }
};

/**
 * Stop polling for messages.
 */
Consumer.prototype.stop = function () {
  debug('Stopping consumer');
  this.stopped = true;
};

Consumer.prototype._poll = function () {
  var messagesToFetch = this.queue.concurrency - this.queue.running();

  var receiveParams = {
    QueueUrl: this.queueUrl,
    MaxNumberOfMessages: messagesToFetch,
    WaitTimeSeconds: 20
  };

  if (!this.stopped && messagesToFetch > 0) {
    debug('Polling for messages');
    this.sqs.receiveMessage(receiveParams, this._handleSqsResponse.bind(this));
  }
};

Consumer.prototype._processMessage = function (message, cb) {
  var consumer = this;

  this.emit('message_received', message);
  async.series([
    function handleMessage(done) {
      consumer.handleMessage(message, done);
    },
    function deleteMessage(done) {
      consumer._deleteMessage(message, done);
    }
  ], function (err) {
    if (err) {
      consumer.emit('error', err);
      return cb(err);
    }

    consumer.emit('message_processed', message);
    cb();

    consumer._poll();
  });
};

Consumer.prototype._handleSqsResponse = function (err, response) {
  if (err) this.emit('error', err);

  var consumer = this;

  debug('Received SQS response');
  debug(response);
  if (response && response.Messages) {
    response.Messages.forEach(function (message) {
      consumer.queue.push(message);
    });
  } else {
    this._poll();
  }
};

Consumer.prototype._deleteMessage = function (message, cb) {
  var deleteParams = {
    QueueUrl: this.queueUrl,
    ReceiptHandle: message.ReceiptHandle
  };

  debug('Deleting message %s', message.MessageId);
  this.sqs.deleteMessage(deleteParams, cb);
};

module.exports = Consumer;