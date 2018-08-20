# Focaccia AWS Adapter

This is an AWS s3 adapter for focaccia filesystem abstraction layer.

## Installation

Execute `npm install --save @focaccia/aws-adapter` into your main repo.

## How to use.

**Uploading a file using AWS adapter**
```javascript
const AWS = require('aws-sdk');
const {AwsS3Adapter} = require("@focaccia/aws-adapter");
const {Focaccia} = require("@focaccia/focaccia");

AWS.config.update({
  accessKeyId: 'something',
  secretAccessKey: 'something',
});

var s3 = new AWS.S3();

let tAsty = new Focaccia(new AwsS3Adapter(s3, 'newBucket'), {});

// This will upload a file
let res = tAsty.write("helloworld.txt", "Hello Amazon world");

// Return a promise
res.then((d) => {
   console.log("RESULT", d);
})
```
