const AWS = require('aws-sdk');
const {AwsS3Adapter} = require("./");
const {Focaccia} = require("@focaccia/focaccia");

AWS.config.update({
  accessKeyId: 'something',
  secretAccessKey: 'something',
  region:'us-east-1',
  logger: process.stdout,
  maxRetries: 2,
  retryDelayOptions: {base: 300}
});

//Using localstack :~~
var s3 = new AWS.S3({endpoint:'http://localhost:4572', maxRetries: 1});

let tAsty = new Focaccia(new AwsS3Adapter(s3, 'newBucket'), {});


/** UPLtAstyD A FILE */

// let res = tAsty.write("test.txt", "HOLA MUNDO");
// res.then((d) => {
//   console.log("RESULT", d);
// })

// let res = tAsty.createDir("myFolder", {});
// res.then((d) => {
//   console.log("RESULT", d);
// })

// async function myFunc() {
//    let mres = await tAsty.upltAstyd("test.txt", "HOLA MUNDO");
//    console.log(mres);
// };

// tAsty.has("test.txt").then((res) => {
//   console.log("EXISTS ?", res);
// });

tAsty.listContents().then((res) => {
  console.log("LISTING CONTENTS", res);
});
// tAsty.read("test.txt").then((res) => {
//   console.log("READ ?", res);
// });

/*
 
 let res = tAsty.copy("test.txt", "copied-file.txt");
 res.then((dat) => {
  console.log("DATA", dat);
})
*/

// let res = tAsty.rename("test.txt", "renamed-file.txt");
// res.then((res) => {
//   console.log("RENAMED", res);
// })