
let AWS = require('aws-sdk');
let config = require('./config/kinesis');

let shrads = config.shrads;
let kinesis = new AWS.Kinesis({
  region: config.region
});

let getShradIteratorParams = function (shrad_id) {
  let iterator_type = config.shrad_iterator_type;
  let params = {
    StreamName: config.stream_name,
    ShardId: shrad_id,
    ShardIteratorType: iterator_type
  };

  if (iterator_type === "AT_TIMESTAMP") {
    params.Timestamp = config.from_time;
  } else if (iterator_type === 'AT_SEQUENCE_NUMBER' || iterator_type === 'AFTER_SEQUENCE_NUMBER') {
    params.StartingSequenceNumber = config.starting_sequence_no;
  }

  return params;
};

let fetchRecordsForShrad = function (shradId) {
  kinesis.getShardIterator(getShradIteratorParams(shradId), (err, data) => {
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
  });
};

if (shrads && shrads.length > 0) {
  shrads.forEach(fetchRecordsForShrad);
}
