
const { BaseAdapter, CONSTANTS, Utils } = require("@focaccia/focaccia");
const {stringUtils} = Utils;
const { ACL } = CONSTANTS;

const RESULT_MAP = {
    "Body": "contents",
    "ContentLength": "size",
    "ContentType": "mimetype",
    "Size": "size",
    "Metadata": "metadata",
    "StorageClass": "storageclass",
    "ETag": "etag",
    "VersionId": "versionid",
    "Key": 'name',
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
    async write(path, contents, config = {}) {
        return await this.upload(path, contents, config);
    }

    /**
     * Update a new file
     * @param {string} path 
     * @param {string} contents 
     * @param {object} config 
     */
    async update(path, contents, config = {}) {
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
     * @param {string} path 
     * @param {boolean} bIsFolder 
     */
    async delete(path, bIsFolder = false) {
        let location = this.applyPathPrefix(path);

        if (bIsFolder) {
            location = `${location}/`;
        }

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
     * @param {string} dirname 
     */    
    async deleteDir(dirname) {
        return await this.delete(dirname, true);
    }

    /**
     * Creates a directory
     * @param {string} dirname 
     */    
    async createDir(dirname, config) {
        return await this.upload(dirname + '/', '', config);
    }

    /**
     * Checks if a file exists
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
     * Read a file
     * 
     * @param {string} path 
     */
    async read(path) {
        let response = await this.__readObject(path);

        if (response !== false) {
            response["contents"] = response["contents"].toString();
        }

        return response;
    }

    /**
     * List content of a bucket
     * @param {string} directory 
     * @param {boolean} recursive 
     */
    async listContents(directory = "", recursive = false) {
        let location = "";
        
        if (directory.length > 0) {
            location = this.applyPathPrefix(stringUtils.trim(directory, "/")) + "/";;
        }
        
        let params = {
            "Bucket": this.bucket,
            "Prefix": stringUtils.ltrim(location, "/"),
            "MaxKeys": 999999
        };

        if (recursive === false) {
            params["Delimiter"] = "/";
        }
          
        //Return Emulate directory listing
        let response = await this.retrievePaginatedListing(params);
        let normalized = response.map((item) => {
            return this.__normalizeResponse(item);
        });
        
        return normalized.map(fp => {

            let fullPathName = '';
            if (fp.type === 'dir') {
                fullPathName = fp.path;
            }

            if (fp.name) {
                fullPathName = fp.name;
            }

            if (fullPathName.indexOf(location) === 0) {
                fullPathName = fullPathName.substr(location.length);
            }
            return fullPathName;
        });
    }

    /**
     * Retrieves paginated listing
     * @param {object} options 
     */
    async retrievePaginatedListing(params = {}) {

        let result = [];
        let response = {};

        try {
            response = await new Promise((resolve, reject) => {
              this.s3Client.listObjectsV2(params, (err, data) => {
                  if (err) reject(err);
                  else resolve(data);
              });
          });
        } catch (e) {
            return result;
        }

        if (response["Contents"]) {
            result.push(...response["Contents"]);
        }

        if (response["CommonPrefixes"]) {
            result.push(...response["CommonPrefixes"]);
        }

        return result;
    }

    /**
     * Get metadata of an object
     * @param {string} path 
     */
    async getMetadata(path) {
        let defaults = {"Bucket": this.bucket, "Key": this.applyPathPrefix(path)};
        let params = {...defaults, ...this.options};
        let response = await this.__executeS3Command("headObject", params);

        return this.__normalizeResponse(response, path);
    }

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
     * @param {string} path 
     * @param {object} resource 
     * @param {object} config 
     */
    async writeStream(path, resource, config) {
        return await this.upload(path, resource, config);
    }

    /**
     * Update a file using streams
     * @param {string} path 
     * @param {object} resource 
     * @param {object} config 
     */
    async updateStream(path, resource, config) {
        return await this.upload(path, resource, config);
    }

    /**
     * Copy a file
     * @param {string} path 
     * @param {string} newpath 
     */
    async copy(path, newpath) {

        let acl = this.__getRawVisibility(path) === ACL.VISIBILITY_PUBLIC ? "public-read" : "private";

        let params = {
            "Bucket": this.bucket,
            "Key": this.applyPathPrefix(newpath),
            "CopySource": decodeURI(`${this.bucket}/${this.applyPathPrefix(path)}`),
            "ACL": acl
        };

        params = {...params, ...this.options};

        let response = await this.__executeS3Command("copyObject", params);
        let bExists = await this.has(newpath)

        return bExists;
    }

    /**
     * Read a file stream
     * @param {string} path 
     */
    readStream(path) {
        response = this.__readObject(path);
        
        if (response === false) {
            return false;
        }

        response["stream"] = response["contents"];

        return response;
    }

    /**
     * Read an object and normalize
     * @param {string} path 
     */
    async __readObject(path) {
        let params = {
            "Bucket": this.bucket,
            "Key": this.applyPathPrefix(path)
        };

        let response = {};
        
        try {
            response = await this.__executeS3Command("getObject", {...params, ...this.options});
        } catch (e) {
            return false;
        }

        return this.__normalizeResponse(response, path);
    }

    /**
     * Set visibility for an object
     * @param {string} path 
     * @param {string} visibility 
     * @TODO: Implementation
     */
    setVisibility(path, visibility) {}

    /**
     * Get visibility of an object
     * @TODO: Implementation
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

        let newObject = {...result, ...response};
        
        return this.__mapResult(newObject);
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

        if (Object.keys(newResult).length === 0 && newResult.constructor === Object) {
            return result;
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
                this.s3Client.listObjectsV2({...params, ...this.options}, (err, data) => {
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