/*
 * 1行1メッセージが書かれているファイルを読み込み、キューに入れるスクリプト
 */

const AWS = require('aws-sdk');
const fs = require('fs');

// Create an SQS service object
const sqs = new AWS.SQS({apiVersion: '2012-11-05', region: 'ap-northeast-1'});

/**
 * sqs.sendMessageBatch()を使ってキューにメッセージを送る。
 * Failedが返ってきた場合は1回だけリトライする。
 * リトライも失敗した場合は例外を投げる。
 */
function sendMessageBatch(queueUrl, messageBodies) {
  return new Promise(async (resolve, reject) => {
    if (messageBodies.length > 10) {
      reject(`messageBodies.length > 10 : ${messageBodies}`);
    }
    if (messageBodies.length == 0) {
      return resolve(messageBodies);
    }

    const TRY_MAX = 2;
    let tryCount = 0;
    const allEntries = messageBodies.map(function(messageBody, index) {
      return {
        Id: String(index),
        MessageBody: messageBody,
      };
    });
    let entries = allEntries;

    while (true) {
      tryCount += 1;
      var params = {
        Entries: entries,
        QueueUrl: queueUrl,
      };
      let result = await sqs.sendMessageBatch(params).promise();
      if (result.Failed.length == 0) {
        resolve(messageBodies);
        return;
      }
      if (tryCount >= TRY_MAX) {
        throw new Error('tryCount >= TRY_MAX ' + result.Failed);
      }
      entries = result.Failed.map(function(failed) {
        var id = Number(failed.Id);
        return allEntries[id];
      });
    }
  });
}

/**
 * 1行1メッセージが書かれているファイルを読み込み、キューに入れる。
 *
 * @param {String} filename 例: 'list.txt'
 * @param {String} queueUrl 例: 'https://sqs.ap-northeast-1.amazonaws.com/XXXXXXXXXXXXXXX/MY-QUEUE-NAME'
 */
function addEachLineToQueue(filename, queueUrl) {
  return new Promise(async (resolve, reject) => {
    let lines = fs.readFileSync(filename).toString().split('\n').map(line => line.replace(/\s+$/g, '')).filter(line => line != '');
    const PROMISES_SIZE = 100;
    let i = 0;
    while (i < lines.length) {
      let promises = [];
      for (let j = 0; j < PROMISES_SIZE && i < lines.length; j++) {
        let messageBodies = lines.slice(i, i + 10);
        promises.push(sendMessageBatch(queueUrl, messageBodies));
        i += 10;
      }
      let results = await Promise.all(promises);
      var time = new Date();
      results.forEach(function(messageBodies) {
        console.log(time.toISOString(), '   ', messageBodies.join(','));
      });
    }
    resolve();
  });
}

async function main() {
  if (process.argv.length - 2 < 2) {
    console.log(`Usage: node ${process.argv[1]} QUEUE_URL FILENAME`);
    process.exit(1);
    return;
  }

  const QUEUE_URL = process.argv[2];
  const FILENAME = process.argv[3];

  var started = new Date();

  await addEachLineToQueue(FILENAME, QUEUE_URL);

  var finished = new Date();
  console.log("Elapsed:" + (finished - started) + " ms");
}

main();
