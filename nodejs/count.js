const AWS = require('aws-sdk');

// Create an SQS service object
var sqs = new AWS.SQS({apiVersion: '2012-11-05', region: 'ap-northeast-1'});

function getMessageCount(queueUrl) {
  var params = {
    QueueUrl: queueUrl,
    AttributeNames: [
      'All'
    ]
  };
  sqs.getQueueAttributes(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      console.log('ApproximateNumberOfMessages', data.Attributes.ApproximateNumberOfMessages);
      console.log('ApproximateNumberOfMessagesNotVisible', data.Attributes.ApproximateNumberOfMessagesNotVisible);
      console.log('ApproximateNumberOfMessagesDelayed', data.Attributes.ApproximateNumberOfMessagesDelayed);
    }
  });
}

async function main() {
  if (process.argv.length - 2 < 1) {
    console.log(`Usage: node ${process.argv[1]} QUEUE_URL`);
    process.exit(1);
    return;
  }
  const QUEUE_URL = process.argv[2];
  getMessageCount(QUEUE_URL);
}

main();
