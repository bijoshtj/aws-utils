
let AWS = require('aws-sdk');
let config = require('./config/kinesis');

let shrads = config.shrads;
let kinesis = new AWS.Kinesis({
  region: config.region
});

let getShrads = function () {
  return kinesis.listShards({
      StreamName: config.stream_name
    })
    .promise()
    .then(data => {
      if (data && data.Shards && data.Shards.length > 0) {
        return data.Shards.map(curr => curr.ShardId);
      }
    });
};

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

let getShradIterator = function (shrad_id) {
  return kinesis.getShardIterator(getShradIteratorParams(shrad_id))
    .promise();
};

let getRecords = function (shrad_iter) {
  return kinesis.getRecords(shrad_iter)
    .promise();
};

let fetchRecordsForShrad = function (shradId) {
  return getShradIterator(shradId)
    .then(getRecords)
    .then(data => {
      console.log('shrad id: ', shradId);

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
    });
};


getShrads()
  .then(shrad_ids => {
    let promise_arr = shrad_ids.map(fetchRecordsForShrad);

    return Promise.all(promise_arr);
  })
  .then(res => {
    console.log('successfully completed fetch');
  })
  .catch(ex => {
    console.log('error', ex);
  });
