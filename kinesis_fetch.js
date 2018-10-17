
let AWS = require('aws-sdk');
let config = require('./config/kinesis');

var kinesis = new AWS.Kinesis({
  region: config.region
});

kinesis.getShardIterator({
  ShardId: config.shradId, /* required */
  //ShardIteratorType: 'TRIM_HORIZON', /* required */
  ShardIteratorType: config.shrad_iterator_type, /* required */
  StreamName: config.stream_name, /* required */
  Timestamp: config.from_time
}, (err, data) => {
  if (!err) {
    console.log('shrad iterator resp: ', data);
    kinesis.getRecords(data, (err, data) => {
      if (!err) {
        console.log("========= get records =========");
        if (data && data.Records && data.Records.length > 0) {
            console.log("Total records in egress kinesis: ", data.Records.length);
          for (let i = 0; i < data.Records.length; i++) {
            let curr = data.Records[i];

            console.log("\n\n+++++ Record: ", i+1, " ++++++");
            console.log("PartitionKey: ", curr.PartitionKey);
            console.log("ArrivalTime: ", curr.ApproximateArrivalTimestamp);
            console.log("Data: ", curr.Data.toString('utf8'));
          }
        } else {
          console.log("No records found!!!!");
        }
      } else {
        console.log('error get records: ', err);
      }
    });
  } else {
    console.log('get shrad iteratoor error ', err);
  }
})
