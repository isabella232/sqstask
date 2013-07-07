var util   = require('util')
  , crypto = require('crypto')
  , _      = require('underscore');

var sqs         = {} // This will be set in the module nitializer
  , queuePrefix = {}; // As will be this.



/**
 * Pack and unpack are here to keep whatever shell we use for messages all in
 * one place; this shell shouldn't be visible outside this module.
 */
var pack = function(message) {
  var body = {message: message} // doing this so we can attach other metadata if necessary
  return util.format('%j', body);
};

/**
 * Pack and unpack are here to keep whatever shell we use for messages all in
 * one place; this shell shouldn't be visible outside this module.
 */
var unpack = function(bodyString) {
  try {
    var body = JSON.parse(bodyString);
    if (typeof body.message !== "undefined") {
      return body.message;
    } else {
      var err = new Error("Didn't get a message; instead got:" + bodyString);
      return err;
    }
  } catch (err) {
    return err;
  }
};

/**
 * Straightforward: send a message to the queue.  We do a little checking to
 * ensure that it worked (since SQS sends us an MD5 hash of the message), but no
 * information is returned except 'true', unless something went wrong that we
 * might be able to fix up the chain.
 */
exports.put = function(message, callback) {
  var body    = pack(message)
    , options = { QueueName   : publishQueue
                , MessageBody : body
                }
    , hasher  = crypto.createHash('md5');
  hasher.update(body, 'utf8');
  var md5 = hasher.digest('hex');
  sqs.SendMessage(options, function(err, data) {
    if (err) {
      callback(err);
    } else if (  typeof data.Body.SendMessageResponse === "undefined"
              || typeof data.Body.SendMessageResponse.SendMessageResult === "undefined"
              || typeof data.Body.SendMessageResponse.SendMessageResult.MD5OfMessageBody === "undefined"
              || data.Body.SendMessageResponse.SendMessageResult.MD5OfMessageBody !== md5) {
      err = new Error('Message corrupted; did not successfully send!');
      callback(err);
    } else {
      callback(null, true);
    }
  });
};

/**
 * Receives a message and passes it to the callback.  Callbacks that need to
 * delete the message received after processing must accept three parameters,
 * the third being the thunk to call to delete the message once that is needed.
 * Parameters: 
 *  err - no surprise
 *  message - the JSON message
 *  delFunc - the thunk that can be called to delete this message
 *
 * Right now there is no way to get a ReceiptHandle except contained by the
 * thunk. We can make a lower level exports.getRaw or something if we need other
 * metadata.
 *
 * It would also be convenient if we could pass in a visibility timeout and
 * expect that the thunk would fail without trying if it was past the timeout,
 * since relying on it to work after that is dangerous.
 */
exports.get = function(callback) {
  var options = {QueueName: publishQueue};
  sqs.ReceiveMessage(options, function(err, data) {
    var del     = null
      , message = null;

    if (err) {
      callback(err);
      return;
    }

    if (  typeof data.Body.ReceiveMessageResponse !== "undefined"
              && typeof data.Body.ReceiveMessageResponse.ReceiveMessageResult !== "undefined"
              && typeof data.Body.ReceiveMessageResponse.ReceiveMessageResult.Message !== "undefined") {

      if (typeof data.Body.ReceiveMessageResponse.ReceiveMessageResult.Message.Body !== "undefined" ) {
        message = unpack(data.Body.ReceiveMessageResponse.ReceiveMessageResult.Message.Body);
      }

      if (typeof data.Body.ReceiveMessageResponse.ReceiveMessageResult.Message.ReceiptHandle !== "undefined" ) {
        del = _.partial(exports.del, [data.Body.ReceiveMessageResponse.ReceiveMessageResult.Message.ReceiptHandle]);
      }
    }
    callback(null, message, del);
  });
};

/**
 * Deletes a message from the queue. Mostly used by the thunk returned from
 * exports.get.
 */
exports.del = function(receiptHandle, callback) {
  var options = { QueueName     : publishQueue
                , ReceiptHandle : receiptHandle
                };
  sqs.DeleteMessage(options, function(err, data) {
    if (err) {
      callback(err, false);
    } else if (data.StatusCode !== 200) {
      err = new Error('Delete failed for some unknown reason!');
      callback(err, false);
    } else {
      callback(null, true);
    }
  });
};

/**
 * Call handle with a function that can take the following parameters:
 *  err - no surprise
 *  message - the JSON message
 *  delFunc - the thunk that can be called to delete this message
 *  callback - the callback to indicate processing is finished for this call
 */
exports.handle = function(handler) {
  var shouldStop = false;
  var stopping = new Error('Stop was called.');

  var stop = function stop() {
    shouldStop = true;
  };

  var inner = function() {
    if (shouldStop) { return handler(stopping); }
    exports.get(function queueJobHandlerCallback(err, message, del) {
      handler(err, message, del, inner);
    });
  };
  inner();
  return stop;
};



/**
* Module initializer
*
* @param AWSConfig - configuration to connect to SQS on AWS.
*   Required fields:
*    - awsAccountId : AWS Account ID.
*    - accessKeyId : AWS access key ID.
*    - secretAccessKey : AWS secret access key.
*    - region. This value must correspond to the ones defined in awssum.io library's amazon/amazon module: http://awssum.io/amazon/
               Region defaults to amazon.US_EAST_1, since it still seems to be the most common one.
*    - prefix: A unique prefix that will be used in combination with topic names to name queues used in Task Scheduler. Defaults to: "task_"            
*
* @return 
*   true: if success, err object: if SQS connection cannot be initialized or another error occured.
*
*/
exports = module.exports = function(AWSConfig) {

  var awssum = require('awssum')
    , amazon = require('awssum-amazon')
    , Sqs = require('awssum-amazon-sqs').Sqs;

  // Defined at the module level, initialized here:
  queuePrefix = AWSConfig.prefix || "task_";
  
  // Defined at the module level, initialized here:
  sqs = new Sqs({
    'accessKeyId'     : AWSConfig.accessKeyId
  , 'secretAccessKey' : AWSConfig.secretAccessKey
  , 'awsAccountId'    : AWSConfig.awsAccountId
  , 'region'          : AWSConfig.region || amazon.US_EAST_1
  });
  
  console.log("Sucessfully connected");
  
};