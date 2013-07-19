var util   = require('util')
  , crypto = require('crypto')
  , _      = require('underscore');

var sqs             = {}  // This will be set in the module nitializer
  , queueAttributes = {}  // As will be this.
  , queuePrefix     = ''; // And as will be this.

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
*    - prefix: A unique prefix that will be used in combination with topic names to name queues used in Task Scheduler. Defaults to: "task"  
*    - AttributeName[] and AttributeValue[]: allows configuration of topic queue values in param-array-set syntax. 
        @See: http://awssum.io/amazon/sqs/create-queue.html for the syntax
        @See: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Query_QueryCreateQueue.html for allowed values
*          
*
* @return 
*   true: if success, err object: if SQS connection cannot be initialized or another error occured.
*
*/
exports = module.exports = function(AWSConfig) {

  // Ensure proper initialization
  if (!AWSConfig || typeof AWSConfig.accessKeyId === 'undefined' ||
                    typeof AWSConfig.secretAccessKey === 'undefined' ||
                    typeof AWSConfig.awsAccountId === 'undefined') {
  
    throw new Error("You must initialize sqstask module with a configuration object that has properly \
defined accessKeyId, secretAccessKey and awsAccountID fields, coresponding to core AWS credentials");

  }
  
  if (typeof AWSConfig.AttributeName !== 'undefined' && typeof AWSConfig.AttributeValue !== 'undefined') {
    queueAttributes.names  = AWSConfig.AttributeName;
    queueAttributes.values = AWSConfig.AttributeName;
  } else {
    queueAttributes.names  = ['MessageRetentionPeriod', 'VisibilityTimeout'];
    queueAttributes.values = [ 3600  // 1 hour retention
                             , 120   // 3 minutes visibility timeout. Most jobs should complete in under 2 minutes.
                             ];    
  }

  var awssum = require('awssum')
    , amazon = require('awssum-amazon')
    , Sqs = require('awssum-amazon-sqs').Sqs;

  // Defined at the module level, initialized here:
  queuePrefix = AWSConfig.quequePrefix || "task";
  
  // Defined at the module level, initialized here:
  var options = {
    'accessKeyId'     : AWSConfig.accessKeyId
  , 'secretAccessKey' : AWSConfig.secretAccessKey
  , 'awsAccountId'    : AWSConfig.awsAccountId
  , 'region'          : AWSConfig.region || amazon.US_EAST_1
  };
  
  sqs = new Sqs(options);
  
  return exports;
  
};

/**
 * Straightforward: send a message to the queue.  We do a little checking to
 * ensure that it worked (since SQS sends us an MD5 hash of the message), but no
 * information is returned except 'true', unless something went wrong that we
 * might be able to fix up the chain.
 */
exports.put = function(topic, message, callback) {
  
  var queueName = queuePrefix + "_" + topic;
      
  var body    = pack(message)
  , options = { QueueName   : queueName
              , MessageBody : body
              }
  , hasher  = crypto.createHash('md5');
  
  hasher.update(body, 'utf8');
  var md5 = hasher.digest('hex');
  
  sqs.SendMessage(options, function sendMessageToQueueCallback(err, data) {
    
    //console.dir("Send message returne! with: ");
    //console.dir(err);
    //console.dir(data);
    
    if (err) {
      callback(new Error(util.inspect(err.Body.ErrorResponse.Error)));
    } else if (  typeof data.Body.SendMessageResponse === "undefined"
              || typeof data.Body.SendMessageResponse.SendMessageResult === "undefined"
              || typeof data.Body.SendMessageResponse.SendMessageResult.MD5OfMessageBody === "undefined"
              || data.Body.SendMessageResponse.SendMessageResult.MD5OfMessageBody !== md5) {
      err = new Error('Message corrupted; did not successfully get sent!');
      callback(err);
    } else {
      callback(null);
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
exports.get = function(topic, callback) {
  var options = {QueueName: queuePrefix + "_" + topic};
  sqs.ReceiveMessage(options, function getMessageFromQueueCallback(err, data) {
    var receiptHandle   = ""
      , message         = null;

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
        receiptHandle = data.Body.ReceiveMessageResponse.ReceiveMessageResult.Message.ReceiptHandle;
      }
    }
    
    var oMessage        = new exports.Message;
        oMessage.body   = message;
        oMessage.id     = receiptHandle;
        oMessage.topic  = topic;
    callback(null, oMessage);
  });
};

/**
* Check if a topic exists, create if it doesn't
*
* This is not error-proof, because per AWS: 
* "You must wait 60 seconds after deleting a queue before you can create another with the same name."
*/
exports.topicEnsureExists = function (topic, callback) {

  var name = queuePrefix + "_" + topic
  
  queueExists(name, function ensureQueueCheckCallback(err) {
    if (err) { // Probably doesn't exist, let's try to create
      queueCreate(name, function ensureQueueCreateCallback(err2) {
        if (err2) {
          callback(new Error(util.inspect(err2.Body.ErrorResponse.Error)));
        } else {
          callback(null);
        }
        return;
      });
      return;
    }
    callback(null);
  });
}


exports.Message = function (topic, body, id) {
  this.topic = topic || null;
  this.body = body || "";
  this.id   = id || null;
}

exports.Message.prototype.del     = function(callback) {

  if (!this.topic) { callback(new Error("Cannot delete a message with empty topic.")); return; }
  if (!this.id)    { callback(new Error("Cannot delete a message with empty ID.")); return; }
      
  deleteMessage(this.topic, this.id, function(err) {
    callback(err);
    return 
  });

};

exports.Message.prototype.release = function(callback) {
  self = this;
  
  // Unfortunately, SQS has no concept of releasing a locked message
  // so to we ought to to delete and re-send the message :(
  deleteMessage(self.topic, self.id, function releaseDeleteCallback(err) {
    if (!err) {      
      exports.put(self.topic, self.body, function releaseResendCallback(err2) {
        
        // After releasing, current message seizes to exist!
        self.topic = "";
        self.body = "";
        self.id = null;
        
        callback(err2);
        return;
      });
    } else {
      callback(new Error(util.inspect(err.Body.ErrorResponse.Error)));
      return;
    }
  });
};

//---- Private functions. 
//---- These are defined as regular functions so that they don't need to be declared before they are used!

function deleteMessage(topic, receiptHandle, callback) {
  var options = { QueueName     : queuePrefix + "_" + topic
                , ReceiptHandle : receiptHandle
                };
  sqs.DeleteMessage(options, function(err, data) {
    if (err) {
      callback(new Error(util.inspect(err.Body.ErrorResponse.Error)));
    } else if (data.StatusCode !== 200) {
      err = new Error('Delete failed for some unknown reason!');
      callback(err);
    } else {
      callback(null);
    }
  });
}

function queueExists(name, callback) {
  var params = {
    QueueName       : name
  , AttributeName   : ['CreatedTimestamp']
  };
  
  sqs.GetQueueAttributes(params, function checkQueueExistsCallback(err, data) {
    callback(err);
  });
}

function queueCreate(name, callback) {

  var params = {
      QueueName       : name
    , AttributeName   : queueAttributes.names
    , AttributeValue  : queueAttributes.values
  };
  
  sqs.CreateQueue(params, function createQueueCallback(err, data) {
    callback(err);
  });
}


/**
 * Pack and unpack are here to keep whatever shell we use for messages all in
 * one place; this shell shouldn't be visible outside this module.
 */
function pack(message) {
  var body = {message: message} // doing this so we can attach other metadata if necessary
  //return JSON.stringify(body);
  return util.format('%j', body);
};

/**
 * Pack and unpack are here to keep whatever shell we use for messages all in
 * one place; this shell shouldn't be visible outside this module.
 */
function unpack(bodyString) {
  try {
    var body = JSON.parse(bodyString);
    if (typeof body.message !== "undefined") {
      return body.message;
    } else {
      var err = new Error("Didn't get a message; instead got:" + bodyString);
      return err;
    }
  } catch (err) {
    return new Error("Error parsing JSON message from the queue. Probably invalid JSON: " + bodyString);
  }
};