const AWS = require('aws-sdk')
const {AwsS3Adapter} = require("./");

AWS.config.update({
  accessKeyId: 'something',
  secretAccessKey: 'something',
  region:'us-east-1',
  logger: process.stdout
});

//Using localstack :~~
var s3 = new AWS.S3({endpoint:'http://localhost:4572'});

let oA = new AwsS3Adapter(s3, 'newBucket');

/** UPLOAD A FILE */

// let res = oA.upload("test.txt", "HOLA MUNDO");
// res.then((d) => {
//   console.log("RESULT", d);
// })



/*
 
 let res = oA.copy("test.txt", "copied-file.txt");
 res.then((dat) => {
  console.log("DATA", dat);
})
*/

let res = oA.rename("test.txt", "renamed-file.txt");
res.then((res) => {
  console.log("RENAMED", res);
})