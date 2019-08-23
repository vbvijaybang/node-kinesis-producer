"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const AWS = require("aws-sdk");
const _ = require("lodash");
const async = require("async");
const hyperid = require("hyperid");
const pLimit = require('p-limit');
const promiseLimit = pLimit(10);
const EventEmitter = require('events');

const DefaultKinesisProducerOptions = {
    streamName: null,
    schema: null,
    kinesisClient: null,
    maxRecords: 400,
    maxTime: 0,
    chunkSize: 100,
    kinesisOptions: null
};
class KinesisProducer extends EventEmitter {
    constructor(options, application) {
        super();
        this.MAX_ARRAY_LIMIT = 499;
        this.isDrainInProgress = false;
        this.options = Object.assign({}, DefaultKinesisProducerOptions, options);
        if (options.chunkSize && options.chunkSize > this.MAX_ARRAY_LIMIT) {
            console.log(`Max chunk size for kinesis putRecords is ${this.MAX_ARRAY_LIMIT}. Please choose a chunk value < ${this.MAX_ARRAY_LIMIT}`);
            throw new Error(`Max chunk size for kinesis putRecords is ${this.MAX_ARRAY_LIMIT}. Please choose a chunk value < ${this.MAX_ARRAY_LIMIT}`);
        }
        this.queue = [];
        console.log("queue: " + this.queue.length);
        this.partitionKeyGen = hyperid();
        if (!this.options.kinesisClient) {
            this.options.kinesisClient = new AWS.Kinesis(this.options.kinesisOptions);
        }
        this.kinesisClient = this.options.kinesisClient;
        if (this.options.maxTime > 0) {
            setInterval(() => {
                if (!this.isDrainInProgress && this.queue.length > 0) {
                    this.drainAsync();
                }
            }, this.options.maxTime);
        }

        const classObj = this;
        process.on('SIGINT', () => {
           console.log(`SIGINT cleaning ${this.queue.length}`)
            if (this.queue.length > 0) {
                this.drainAsync();
            }
        });
        process.on('beforeExit',() => {
            console.log(`before exit cleaning ${this.queue.length}`)
            if (!this.isDrainInProgress && this.queue.length > 0) {
                this.drainAsync();
            }
        });
    }

    async drainAsync() {
        await this.drain();
    }

    async putRecords(records) {
        this.queue = this.queue.concat(records);
        if (this.queue.length >= this.options.maxRecords && !this.isDrainInProgress) {
            this.drain();
        }
    }

    formatRecord(record) {
        delete record.retryCount;
        return {
            Data: JSON.stringify(record),
            PartitionKey: this.partitionKeyGen()
        };
    }

    drain() {
        const classObj = this;
        return new Promise((resolve, reject) => {
            console.log(`Draining records`);
            classObj.isDrainInProgress = true;
            const chunks = _.chunk(classObj.queue.splice(0, classObj.queue.length), classObj.options.chunkSize);
            
            Promise.all(chunks.map(chunk => promiseLimit(() => classObj.drainRecords(chunk)))).then((results) => {
                classObj.isDrainInProgress = false;
                classObj.emit("drainSuccess", results)
                resolve();

            }).catch((errors) => {
                classObj.isDrainInProgress = false;
                classObj.emit("drainError", errors)
                resolve();
            })
        })
    }

    drainRecords(records) {
        console.log('Draining :: ', records.length);
        const failedRecords = [];
        const kRecords = records.map(record => this.formatRecord(record));
        const classObj = this;
        return new Promise((resolve, reject) => {
            classObj.kinesisClient.putRecords({
                "Records": kRecords,
                "StreamName": classObj.options.streamName,
            }, (err, data) => {
                if (err) {
                    console.log(`Error publishing records to Kinesis :: Code = ${err.code}, Message = ${err.message}`, err);
                    reject({
                        unsuccessfulRecords: records
                    });
                }
                else {
                    if (data && data.FailedRecordCount > 0) {
                        console.log(`${data.FailedRecordCount} records failed to be pushed to Kinesis. Will retry them till maxRetries`);
                        data.Records.forEach((rec, index) => {
                            if (rec.ErrorCode || rec.ErrorMessage) {
                                failedRecords.push(records[index]);
                            }
                        });
                    }
                    if(failedRecords && failedRecords.length > 0) {
                        reject({
                            unsuccessfulRecords: failedRecords
                        });
                    }
                    else {
                        resolve({
                            successfulRecords: records
                        })
                    }
                }
            });

        });
        
    }
}
module.exports = KinesisProducer;
