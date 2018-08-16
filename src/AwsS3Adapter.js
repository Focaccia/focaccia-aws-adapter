
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
    upload(path, contents, config = {}) {
        return this.upload(path, contents, config);
    }

    rename(path, newpath) {

    }

    delete(path) {}
    
    deleteDir(dirname) {}
    createDir(dirname) {}
    has(path) {}
    read(path) {}
    listContents(directory = "", recursive = false) {}
    retrievePaginatedListing(options = []) {}
    getMetadata(path) {}
    getSize(path) {}
    getMimetype(path) {}
    getTimestamp(path) {}
    writeStream(path, resource, config) {}
    updateStream(path, resource, config) {}

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

    readStream(path) {}
    readObject(path) {}
    setVisibility(path, visibility) {}
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
    __doesDirectoryExist(location) {}

    async __executeS3Command(command, params) {
        return await new Promise((resolve, reject) => {
            this.s3Client[command](params, (err, res) => {
                if (res) { resolve(res); }
                if (err) { reject(err); }
            });
        });
    }

}

module.exports = AwsS3Adapter;