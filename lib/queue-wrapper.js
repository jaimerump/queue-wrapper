let debug   = require('debug')("queue-wrapper");
let amqp  = require('amqplib/callback_api');
let _     = require('lodash');

const ROUTING_KEY = "";

/**
 * Constructor
 */
function QueueWrapper( opts ) {

  // Stub options and add any values
  this._options = _.extend({
    host: "",
    queue: "main"
  }, opts || {});

  // main pieces
  this._connection = false;
  this._channel = false;

  debug("Queue Options ", this._options);

}

/**
 * Connects to the queue
 * 
 * @param  Function callback
 */
QueueWrapper.prototype.connect = function( callback ) {
  
  let s = this;

  // Connect to the Queue
  amqp.connect( s._options.host, { heartbeat: 5 }, function( err, conn ) {

    // Check for an error
    if( err ) return callback( "Could not connect to " + s._options.host );

    // store connction
    s._connection = conn;

    // Check the channel
    conn.createChannel( function( err, ch ) {

      // Check for channel error
      if( err ) return callback( "Could not establish a channel." );

      // Store the channel
      s._channel = ch;

      // Attach close handlers
      s._connection.on('close', function() {
        s.handleError();
      });
      s._channel.on('close', function() {
        s.handleError();
      });

      // Make sure queue exists
      s._channel.assertQueue( s._options.queue, { durable: false }, function(err) {

        if(err) return callback("Couldn't assert queue: "+err);

        if( s._options.delayed ) {
          
          let exchange_params = { 
            autoDelete: false, 
            durable: true, 
            passive: true, 
            arguments: { 
              "x-delayed-type": "direct" 
            } 
          };
          ch.assertExchange( s._options.queue+"-exchange", "x-delayed-message", exchange_params, function(err) {

            if(err) return callback("Unable to assert exchange: "+err)

            // Bind them together
            ch.bindQueue( s._options.queue, s._options.queue+"-exchange", ROUTING_KEY, {}, function(err) {
              if(!err) console.log("Connected to delayed queue");
              return callback(err);
            }); // ch.bindQueue

          }); // ch.assertExchange

        } else {
          // We are good
          console.log("Connected to queue");
          callback( false );
        }

      }); // channel.assertQueue
      
    }); // conn.createChannel

  }); // amqp.connect

}

/**
 * Enqueues a payload immediately
 * 
 * @param  Object   payload   The payload to put onto the queue
 * @param  Function callback
 */
QueueWrapper.prototype.send = function( payload, callback ) {
  
  let s = this;

  let mess = JSON.stringify(payload);

  // Channel assertion
  s._channel.assertQueue( s._options.queue, { durable: false }, function(err) {

    if(err) return callback("Couldn't assert queue: "+err);

    // Send the Message
    let successful = s._channel.sendToQueue( s._options.queue, new Buffer( mess ) );

    if( successful ) {
      return callback() 
    } else {
      console.log("Unable to write to queue");
      return callback("Unable to write to queue");
    }

  }); // s._channel.assertQueue

}

/**
 * Enqueues a payload after the given delay
 * 
 * @param  Object   payload   The payload to put onto the queue
 * @param  Integer  delay     How long to delay the event being processed
 * @param  Function callback
 */
QueueWrapper.prototype.delay = function( payload, delay, callback ) {
  return callback();
}

/**
 * Listens to the queue and calls the callback on every event that passes through
 * @param  Function callback
 */
QueueWrapper.prototype.watch = function( callback ) {

  let s = this;

    // Channel assertion
    s._channel.assertQueue( s._options.queue, { durable: false } );

    // Listen for messages
    s._channel.consume( s._options.queue, function( msg ) {

        // check message is valid
        if ( msg !== null ) {
            
            // Parse the Message JSON
            let message = false;
            try {
                message = JSON.parse( msg.content.toString() );
                debug("Payload", message )
            } catch (e) {
                 debug("Payload Parse Error", e )
            }
            
            // Ack before so we can't get stuck in loop
            s._channel.ack( msg );

            // check message, it does happen and needs to be protected against
            if( !message ) {
                // Bad Message
                callback( "could not parse message", false )
            } else {
              // Good Message
              callback( false, message );
            } 

        }
    });
}

/**
 * Tries to reconnect on error
 * @param  [Object|String] err The error that occurred
 */
QueueWrapper.prototype.handleError = function(err) {

  let s = this;

  // Try to reconnect
  s.connect(function(reconnect_err) {

    if(reconnect_err) {
      // Couldn't reconnect, crash and let supervisor deal with it
      let err_message = ( _.isObject(err) ) ? err.message : err;
      console.log(`${s._options.queue} hit error ${err_message} and couldn't reconnect because ${reconnect_err}`);
      throw new Error("Couldn't reconnect to "+s._options.queue+" because: "+reconnect_err); 
    }

  }); // s.reconnect

}

// Set exports
module.exports = QueueWrapper;