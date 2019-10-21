# Document Library Prefetch consumer

A document library specific consumer that will resolve document and add them to the library. 

## Remote Documents
The consumer will handle remote documents and pull them down, store a local copy and then add that to the library

Currently supported is 
* **HTTP/HTTPS**

### Origins
When retrieving remote documents the resulting mongo document will contain an `origin`  property that is an array of 
identifies the remote location/s that the document was retrieved from. Each origin contains the following properties

* **scheme** - the type of origin source usually derived from the source. If the origin does not contain a scheme then a default of file will be assumer 
* **uri** - this is a fully qualified uri i.e. `file:///path/to/file` or `https://bbc.co.uk/news`
* **headers** - optional: where required will contain header data about the origin
* **metadata** - optional: where required will contain additional data derived from the remote file that is not part of any headers

## Execution

This is a scala application that runs inside the JVM

```bash
java -jar consumer-unarchive.jar
```

## Runtime Configuration

The app allows runtime configuration via environment variables

* **MONGO_USERNAME** - login username for mongodb
* **MONGO_PASSWORD** - login password for mongodb
* **MONGO_HOST** - host to connect to
* **MONGO_PORT** - optional: port to connect to (default: 27017) 
* **MONGO_DATABASE** - database to connect to
* **MONGO_AUTH_DB** - optional: database to authenticate against (default: admin)
* **MONGO_COLLECTION** - default collection to read and write to
* **RABBITMQ_USERNAME** - login username for rabbitmq
* **RABBITMQ_PASSWORD** - login password for rabbitmq
* **RABBITMQ_HOST** - host to connect to
* **RABBITMQ_PORT** - optional: port to connect to (default: 5672)
* **RABBITMQ_VHOST** - optional: vhost to connect to (default: /)
* **RABBITMQ_EXCHANGE** - optional: exchange that the consumer should be bound to
* **UPSTREAM_QUEUE** - optional: name of the queue to consume (default: klein.prefetch)
* **UPSTREAM_CONCURRENT** - optional: number of messages to handle concurrently (default: 1)
* **DOWNSTREAM_QUEUE** - optional: name of queue to enqueue new files to (default: klein.preprocess)
* **DOCLIB_ROOT** - optional: The filesystem root that the document library (defaults: /)
* **DOCLIB_REMOTE_TARGET** - The target location, relative to the DOCLIB_ROOT, to store files retrieved from remote locations
* **DOCLIB_REMOTE_TEMP** - The temporary location, relative to the DOCLIB_ROOT, to store files retrieved from remote locations
* **DOCLIB_ARCHIVE_TARGET** - The location, relative to the DOCLIB_ROOT, to store remote files that are archived via prefetch
* **DOCLIB_LOCAL_TARGET** - The location, relative to the DOCLIB_ROOT, to store local files that are managed via prefetch
* **DOCLIB_LOCAL_TEMP** - A temp folder, relative to the DOCLIB_ROOT, for local files waiting to be added/updated to the document library
* **AWS_ACCESS_KEY_ID** - optional: AWS access key for use when not run withing AWS 
* **AWS_SECRET_ACCESS_KEY** - optional: AWS secret key for use when not run withing AWS

## Messages

Unlike the document library the messages for the prefetch consumer do not utilise a Mongo ID

Messages are composed in JSON using the following properties

* **source** - required: the path or url to the file to be added"
* **origin** - optional: an array of FQ origins (see above)
* **tags** - optional: a list of arbitrary string values that can be used to tag a document
* **metadata** - optional: additional information that will can be utilised in processing documents
* **derivative** - optional: explicit flag to mark this document as a derivative


## Results

Documents on successful completion will contain the following properties

* **source** - location of file stored on file system
* **origin** - an array of FQ origins (see above)
* **attrs** - filesystem attributes of the file
* **derivative** - explicit flag to identify if this is a derivative of another document
* **mimetype** - the mimetype as derived from apache tika
* **hash** - an md5 hash of the file to assist with deduplication
* **origin** - an array of FQ origins (see above)
* **tags** - a list of arbitrary string values that can be used to tag a document
* **metadata** - additional information that will can be utilised in processing documents
* **created** - the date the document was added to the document library
* **updated** - the date the document was last edited 