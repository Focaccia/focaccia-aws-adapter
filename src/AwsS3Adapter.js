
const { BaseAdapter, CONSTANTS } = require("@focaccia/focaccia");
const { ACL } = CONSTANTS;

const RESULT_MAP = {
    "Body": "contents",
    "ContentLength": "size",
    "ContentType": "mimetype",
    "Size": "size",
    "Metadata": "metadata",
    "StorageClass": "storageclass",
    "ETag": "etag",
    "VersionId": "versionid"
}

const META_OPTIONS = [
    'ACL',
    'CacheControl',
    'ContentDisposition',
    'ContentEncoding',
    'ContentLength',
    'ContentType',
    'Expires',
    'GrantFullControl',
    'GrantRead',
    'GrantReadACP',
    'GrantWriteACP',
    'Metadata',
    'RequestPayer',
    'SSECustomerAlgorithm',
    'SSECustomerKey',
    'SSECustomerKeyMD5',
    'SSEKMSKeyId',
    'ServerSideEncryption',
    'StorageClass',
    'Tagging',
    'WebsiteRedirectLocation',
];

const PUBLIC_GRANT_URI = "http://acs.amazonaws.com/groups/global/AllUsers";

class AwsS3Adapter extends BaseAdapter {
    constructor(client, bucket, prefix = "", options = {}) {

        super(prefix);

        this.s3Client = client;
        this.bucket = bucket;
        this.options = options;
    }

    /**
     * Get the S3 bucket name
     * @returns {string}
     */
    getBucket() {
        return this.bucket;
    }

    /**
     * Set the S3 bucket name
     * @param {string} bucket 
     */
    setBucket(bucket) {
        this.bucket = bucket;
    }

    /**
     * Get AWS S3 Client
     * @returns {S3}
     */
    getClient() {
        return this.client;
    }

    /**
     * Write a new file
     * @param {string} path 
     * @param {string} contents 
     * @param {object} config 
     */
    write(path, contents, config = {}) {
        return this.upload(path, contents, config);
    }

    /**
     * Update a new file
     * @param {string} path 
     * @param {string} contents 
     * @param {object} config 
     */
    update(path, contents, config = {}) {
        return this.upload(path, contents, config);
    }

    /**
     * Write a new file
     * @param {string} path 
     * @param {string} contents 
     * @param {object} config 
     */
    async upload(path, contents, config = {}) {
        return await this.upload(path, contents, config);
    }

    /**
     * Rename an object
     * @param {string} path 
     * @param {string} newpath 
     */
    async rename(path, newpath) {
        
        let copied = await this.copy(path, newpath);

        if (!copied) {
            return false;
        }

        try {
            let deleted = await this.delete(path);
        } catch (e) {
            return false;
        }
        
        return true;
    }

    /**
     * Delete an object
     * @TODO: Implementation
     * @param {string} path 
     */
    async delete(path) {
        let location = this.applyPathPrefix(path);

        let params = {
            "Bucket": this.bucket,
            "Key": location
        };

        await this.__executeS3Command("deleteObject", params);

        let response = await this.has(path);
        return !response;
    }
    
    /**
     * Deletes a directory
     * @TODO: Implementation
     * @param {string} dirname 
     */    
    deleteDir(dirname) {}

    /**
     * Creates a directory
     * @TODO: Implementation
     * @param {string} dirname 
     */    
    createDir(dirname) {}

    /**
     * Checks if a file exists
     * @TODO: Implementation
     * @param {string} path 
     */
    async has(path) {
        let location = this.applyPathPrefix(path);
        
        let fileExists = await this.objectExists(location);

        if (fileExists) {
            return true;
        }

        let response = await this.__doesDirectoryExist(path);
        return response;
    }
    

    /**
     * Read an file
     * @TODO: Implementation
     * @param {string} path 
     */
    read(path) {}

    /**
     * List content of a bucket
     * @TODO: Implementation
     * @param {string} directory 
     * @param {boolean} recursive 
     */
    listContents(directory = "", recursive = false) {}

    /**
     * Retrieves paginated listing
     * @TODO: Implementation
     * @param {object} options 
     */
    retrievePaginatedListing(options = []) {}

    /**
     * Get metadata of an object
     * @TODO: Implementation
     * @param {string} path 
     */
    getMetadata(path) {}

    /**
     * Get size of an object
     * @TODO: Implementation
     * @param {string} path 
     */
    getSize(path) {}

    /**
     * Get mimetype of an object
     * @TODO: Implementation
     * @param {string} path 
     */    
    getMimetype(path) {}

    /**
     * Get timestamp of an object
     * @TODO: Implementation
     * @param {string} path 
     */
    getTimestamp(path) {}

    /**
     * Write a file using streams
     * @TODO: Implementation
     * @param {string} path 
     * @param {object} resource 
     * @param {object} config 
     */
    writeStream(path, resource, config) {}

    /**
     * Update a file using streams
     * @TODO: Implementation
     * @param {string} path 
     * @param {object} resource 
     * @param {object} config 
     */
    updateStream(path, resource, config) {}

    /**
     * Copy a file
     * @param {string} path 
     * @param {string} newpath 
     */
    copy(path, newpath) {

        let acl = this.__getRawVisibility(path) === ACL.VISIBILITY_PUBLIC ? "public-read" : "private";

        let params = {
            "Bucket": this.bucket,
            "Key": this.applyPathPrefix(newpath),
            "CopySource": decodeURI(`${this.bucket}/${this.applyPathPrefix(path)}`),
            "ACL": acl
        };

        return this.__executeS3Command("copyObject", params);
    }

    /**
     * Read a file stream
     * @TODO: Implementation
     * @param {string} path 
     */
    readStream(path) {}

    /**
     * Read an object and normalize
     * @TODO: Implementation
     * @param {string} path 
     */
    readObject(path) {}

    /**
     * Set visibility for an object
     * @param {string} path 
     * @param {string} visibility 
     * @TODO: Implementation
     */
    setVisibility(path, visibility) {}

    /**
     * Get visibility of an object
     * @param {*} path 
     */
    getVisibility(path) {}

    /** 
     * Upload a content to S3
    */
    async upload(path, body, config = {}) {
        let key = this.applyPathPrefix(path);
        let options = this.__getOptionsFromConfig(config);

        let acl = options["ACL"] ? options["ACL"] : "private";

        //@TODO: Implement a tool to guess mimetypes
        //options["mimetype"] = options["mimetype"] ? options["mimetype"] : "GUESS-MIMETYPE";

        //@TODO: Make an util to get the content size from a string and a stream
        // if (!options["ContentLength"]) {
        // options["ContentLength"] = typeof(body) === 'string' ? contentSize(body) : streamSize(body);
        // }

        if (options["ContentLength"] === null) {
            delete options["ContentLength"];
        }

        let params = {
            Bucket: this.getBucket(),
            Body: body,
            Key: key
        };

        let result = await this.__executeS3Command("upload", params);

        return this.__normalizeResponse(result, key);
    }

    /**
     * Map and Get configurations for S3
     * @param {object} config 
     * @returns {object}
     */
    __getOptionsFromConfig(config) {
        let options = this.options;
        let visiblity = config["visibility"];
        let mimetype = config["mimetype"];

        if (visiblity) {
            options["visbility"] = visiblity;
            options["ACL"] = visiblity === ACL.VISIBILITY_PUBLIC ? "public-read" : "private";
        }

        if (mimetype) {
            options["mimetype"] = mimetype;
            options["ContentType"] = mimetype;
        }

        META_OPTIONS.forEach((opt) => {

            if (!config[opt]) {
                return;
            }

            options[opt] = config[opt];

        });

        return options;
    }

    /**
     * Normalize AWS S3 response to a focaccia friendly object
     * 
     * @param {object} response 
     * @param {string} path 
     */
    __normalizeResponse(response, path = null) {

        let result = {
            path: path ? path : this.removePathPrefix(typeof (response["Key"]) === 'string' ? response["Key"] : response["Prefix"]),
            type: "file"
        };

        let lastPos = result.path.length - 1;
        if (result.path[lastPos] === "/") {
            result.path = result.path.substring(0, lastPos);
            result.type = "dir";
        }

        return this.__mapResult({...response, ...result});
    }

    /**
     * Map the result for focaccia
     * 
     * @param {object} result 
     */
    __mapResult(result) {
        
        let newResult = {};

        for (let key in RESULT_MAP) {
            let item = RESULT_MAP[key];
            
            if (result[key]) {
                newResult[item] = result[key];
            } 
        }

        
        return newResult;
    }

    /**
     * Get AWS visibility permission and maps against focaccia
     * 
     * @param {string} path 
     */
    async __getRawVisibility(path) {

        let params = {
            "Bucket": this.bucket,
            "Key": this.applyPathPrefix(path)
        };

        let result = await this.__executeS3Command("getObjectAcl", params);
        let visibility = ACL.VISIBILITY_PRIVATE;

        let {Grants} = result;
        
        if (typeof(Grants) !== 'object') {
            return visibility;
        }

        for (let gk in Grants) {
            let grant = Grants[gk];
            if (
                typeof(grant["Grantee"]["URI"]) !== "undefined"
                && grant["Grantee"]["URI"] === PUBLIC_GRANT_URI
                && grant["Permission"] === "READ"
            ) {
                visibility = ACL.VISIBILITY_PUBLIC;
                break;
            }
        }
    }


    async objectExists(Prefix) {
        
        let params = {
            "Bucket": this.bucket,
            "Prefix": Prefix,
          };
          
          let result = {};

          try {
            result = await new Promise((resolve, reject) => {
                this.s3Client.listObjectsV2(params, (err, data) => {
                    if (err) reject(err);
                    else resolve(data);
                });
            });
          } catch (e) {
              return false;
          }
          
          for (let k in result["Contents"]) {
              let content = result["Contents"][k];

              if (content["Key"] === Prefix) {
                  return true;
              }
          }
        
          return false;
    }

    /**
     * Check if a directory exists
     * @param {string} path 
     */
    async __doesDirectoryExist(path) {
        let Prefix = this.applyPathPrefix(path) + '/';
        let result = await this.objectExists(Prefix);
        return result;

    }

    /**
     * Execute AWS s3 command and generate a promise
     * 
     * @param {string} command 
     * @param {object} params 
     */
    async __executeS3Command(command, params) {

        let res = await this.s3Client[command](params).promise();
        return res;
    }

}

module.exports = AwsS3Adapter;