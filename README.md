# About AWS Kinesis Producer

The KinesisProducer library exported as Node.js modules.

## How To Install:
Using npm

```
$ npm i -g npm
$ npm i --save aws-kinesis-producer
```


## Test Code Example:

```
const KinesisProducer = require("aws-kinesis-producer");


const options = {
    streamName: "test-stream",
    kinesisClient: null,
    maxDrains: 3,
    maxRecords: 1,
    maxTime: 0,
    chunkSize: 400,
    kinesisOptions: {
        region: 'aws region',
        accessKeyId: "Your access key",
        secretAccessKey: "Your secret access key"
    },
};


let kinesisProducer = new KinesisProducer(options);
let record = {
    "message": "Test is test data"
}

kinesisProducer.on('drainError', (errors) => {
    console.log(`error: ${JSON.stringify(errors)}`)
});

kinesisProducer.on('drainSuccess', (records) => {
    console.log(`success records: ${records.length}`)
});

for(let i =0; i < 1; i++) {
    kinesisProducer.putRecords([record]);
}
```
