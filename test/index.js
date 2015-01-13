var Consumer = require('..');
var assert = require('assert');
var sinon = require('sinon');

describe('Consumer', function () {
  var consumer;
  var handleMessage;
  var sqs;
  var response = {
    Messages: [{
      ReceiptHandle: 'receipt-handle',
      MessageId: '123',
      Body: 'body'
    }]
  };

  beforeEach(function () {
    handleMessage = sinon.stub().yieldsAsync(null);
    sqs = sinon.mock();
    sqs.receiveMessage = sinon.stub().yieldsAsync(null, response);
    sqs.receiveMessage.onSecondCall().returns();
    sqs.deleteMessage = sinon.stub().yields(null);
    consumer = new Consumer({
      queueUrl: 'some-queue-url',
      region: 'some-region',
      handleMessage: handleMessage,
      concurrency: 1,
      sqs: sqs
    });
  });

  it('requires a queueUrl to be set', function () {
    assert.throws(function () {
      new Consumer({
        region: 'some-region',
        handleMessage: handleMessage
      });
    });
  });

  it('requires an AWS region to be set', function () {
    assert.throws(function () {
      new Consumer({
        queueUrl: 'some-queue-url',
        handleMessage: handleMessage
      });
    });
  });

  it('requires a handleMessage function to be set', function () {
    assert.throws(function () {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url'
      });
    });
  });

  describe('.start', function () {
    it('fires an error event when an error occurs receiving a message', function (done) {
      var receiveErr = new Error('Receive error');

      sqs.receiveMessage.yields(receiveErr);

      consumer.on('error', function (err) {
        assert.equal(err, receiveErr);
        done();
      });

      consumer.start();
    });

    it('fires an error event when an error occurs deleting a message', function (done) {
      var deleteErr = new Error('Delete error');

      handleMessage.yields(null);
      sqs.deleteMessage.yields(deleteErr);

      consumer.on('error', function (err) {
        assert.equal(err, deleteErr);
        done();
      });

      consumer.start();
    });

    it('fires an error event when an error occurs processing a message', function (done) {
      var processingErr = new Error('Processing error');

      handleMessage.yields(processingErr);

      consumer.on('error', function (err) {
        assert.equal(err, processingErr);
        done();
      });

      consumer.start();
    });

    it('fires a message_received event when a message is received', function (done) {
      consumer.on('message_received', function (message) {
        assert.equal(message, response.Messages[0]);
        done();
      });

      consumer.start();
    });

    it('fires a message_processed event when a message is successfully deleted', function (done) {
      handleMessage.yields(null);

      consumer.on('message_processed', function (message) {
        assert.equal(message, response.Messages[0]);
        done();
      });

      consumer.start();
    });

    it('calls the handleMessage function when a message is received', function (done) {
      consumer.start();

      consumer.on('message_processed', function () {
        sinon.assert.calledWith(handleMessage, response.Messages[0]);
        done();
      });
    });

    it('deletes the message when the handleMessage callback is called', function (done) {
      handleMessage.yields(null);

      consumer.start();

      consumer.on('message_processed', function () {
        sinon.assert.calledWith(sqs.deleteMessage, {
          QueueUrl: 'some-queue-url',
          ReceiptHandle: 'receipt-handle'
        });
        done();
      });
    });

    it('doesn\'t delete the message when a processing error is reported', function () {
      handleMessage.yields(new Error('Processing error'));

      consumer.on('error', function () {
        // ignore the error
      });

      consumer.start();

      sinon.assert.notCalled(sqs.deleteMessage);
    });

    it('consumes another message once one is processed', function (done) {
      sqs.receiveMessage.onSecondCall().yields(null, response);
      sqs.receiveMessage.onThirdCall().returns();

      consumer.start();
      setTimeout(function () {
        sinon.assert.calledTwice(handleMessage);
        done();
      }, 10);
    });

    it('doesn\'t consume more messages when called multiple times', function () {
      sqs.receiveMessage = sinon.stub().returns();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();

      sinon.assert.calledOnce(sqs.receiveMessage);
    });

    it('does not consume more messages at once than the specific concurrency limit', function (done) {
      var concurrentConsumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage: function (message, cb) {
          setTimeout(cb, 10);
        },
        concurrency: 3,
        sqs: sqs
      });

      sqs.receiveMessage = sinon.stub().yieldsAsync(null, response);

      // prevent recursive calls going on forever!
      sqs.receiveMessage.onCall(20).returns();

      concurrentConsumer.start();

      setTimeout(function () {
        sinon.assert.callCount(sqs.receiveMessage, 4);
        done();
      }, 20);
    });
  });

  describe('.stop', function () {
    it('stops the consumer polling for messages', function (done) {
      sqs.receiveMessage.onSecondCall().yieldsAsync(null, response);
      sqs.receiveMessage.onThirdCall().returns();

      consumer.start();
      consumer.stop();

      setTimeout(function () {
        sinon.assert.calledOnce(handleMessage);
        done();
      }, 10);
    });
  });
});