# Document Library Prefetch consumer

A Scala based application that consumes messages from RabbitMQ, processes document files, saves metadata about them in MongoDB and adds them to the file system based library. 

## Remote Documents
The consumer fetches remote documents, stores a local copy of them and adds metadata about any documents processed to the Document Library collection in MongoDB

Currently supported protocols: 
* **HTTP/HTTPS**
* **FTP/SFTP/FTPS**

### Origins
When retrieving remote documents the resulting mongo document will contain an `origin`  property that is an array of 
identifies the remote location/s that the document was retrieved from. Each origin contains the following properties

* **scheme** - the type of origin source usually derived from the source. If the origin does not contain a scheme then a default of file will be assumer 
* **uri** - this is a fully qualified uri i.e. `file:///path/to/file` or `https://bbc.co.uk/news`
* **headers** - optional: where required will contain header data about the origin
* **metadata** - optional: where required will contain additional data derived from the remote file that is not part of any headers

## The Prefetch process

The app is booted via `ConsumerPrefetch` which creates connections to rabbit queues & mongo and then creates 
the `PrefetchHandler` which listens out for messages and does all the work. Each time it gets a new message from rabbit it gets to the `handle` method. This then finds a mongo
record for this document (or creates one) and then starts the document process. This ends up in the method `prefechProcess`
which does most of the heavy lifting.

## Execution

This is a scala application that runs inside the JVM

```bash
java -jar consumer-prefetch.jar
```
A custom config file can be passed to the app using the command line option `--config`.  
Note that the image uses a provided version of `netty.jar` due to version conflicts with other libraries.

## Runtime Configuration

The app allows runtime configuration via environment variables. Most have default values and do not need to be provided. Some have no default values and this is noted in the list.

* **MONGO_USERNAME** - login username for mongodb (default: doclib)
* **MONGO_PASSWORD** - login password for mongodb (default: doclib)
* **MONGO_HOST** - host to connect to (default: localhost)
* **MONGO_PORT** - port to connect to (default: 27017) 
* **MONGO_DOCLIB_DATABASE** - the doclib database (default: doclib)
* **MONGO_AUTHSOURCE** - database to authenticate against (default: admin)
* **MONGO_DOCUMENTS_COLLECTION** - the documents collection (default: documents)
* **MONGO_DERIVATIVES_COLLECTION** - the collection where parent/child references are stored (default: documents_derivatives ie **MONGO_DOCUMENTS_COLLECTION**_derivatives)
* **MONGO_SRV** - whether to use mongodb [DNS seed list](https://docs.mongodb.com/manual/reference/connection-string/) connection. (default: false)
* **MONGO_READ_LIMIT** - how many mongodb simultaneous read connections (default: 50)
* **MONGO_WRITE_LIMIT** - how many mongodb simultaneous write connections (default: 100)
  
* **RABBITMQ_USERNAME** - login username for rabbitmq (default: doclib)
* **RABBITMQ_PASSWORD** - login password for rabbitmq (default: doclib)
* **RABBITMQ_HOST** - host to connect to (optional - no default)
* **RABBITMQ_PORT** - port to connect to (default: 5672)
* **RABBITMQ_VHOST** - vhost to connect to (default: doclib)
* **RABBITMQ_DOCLIB_EXCHANGE or CONSUMER_EXCHANGE** - exchange that the consumer should be bound to (optional - no default)
* **CONSUMER_QUEUE** or **DOCLIB_PREFETCH_QUEUE** - name of the queue to consume (default: prefetch)
* **CONSUMER_CONCURRENCY** - number of messages to handle concurrently (default: 1)
  
* **DOCLIB_SUPERVISOR_QUEUE** - the supervisor queue (default: supervisor)
* **DOCLIB_ERROR_QUEUE** - the errors queue (default: errors)
* **DOCLIB_ARCHIVER_QUEUE** - the archive queue (default: archive)
* **DOCLIB_ROOT** - optional: The filesystem root where the documents are stored (defaults: /)
* **DOCLIB_REMOTE_TARGET** - The target location, relative to the DOCLIB_ROOT, to store files retrieved from remote locations before processing (default: remote-ingress)
* **DOCLIB_REMOTE_TEMP** - The location, relative to the DOCLIB_ROOT, to store files retrieved from remote locations after processing (default: remote)
* **DOCLIB_ARCHIVE_TARGET** - The location, relative to the DOCLIB_ROOT, to store files that are archived via prefetch (default: archive)
* **DOCLIB_LOCAL_TARGET** - The location, relative to the DOCLIB_ROOT, to store local files after processing (default: local)
* **DOCLIB_LOCAL_TEMP** - A folder, relative to the DOCLIB_ROOT, for local files waiting to be processed by the document library (default: ingress)
* **DOCLIB_DERIVATIVE_PATH** - The name of a folder within the **DOCLIB_LOCAL_TARGET** where derivatives of files (eg unzipped, raw text) should be stored (default: derivatives)
* **ADMIN_PORT** - Port that prometheus metrics are exposed on (default: 9090)
* **CONSUMER_NAME** - Name of the consumer (default: prefetch)

## Messages

Messages are composed in JSON using the following properties

* **source** - required: the path or url to the file to be added"
* **origin** - optional: an array of FQ origins (see above)
* **tags** - optional: a list of arbitrary string values that can be used to tag a document
* **metadata** - optional: additional information that will can be utilised in processing documents
* **derivative** - optional: explicit flag to mark this document as a derivative

## Results

On successful completion a document record in MongoDB will contain the following properties

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

## Metrics

The following metrics are exposed via prometheus on port `ADMIN_PORT` (default `9090`):  

* `document_size_bytes` containing `scheme` (ie http or ftp )and `mimetype`.
* `document_fetch_latency` containing `scheme`
* `mongo_latency` containing `consumer` and `operation` (update_document, insert_document, insert_parent_child_mapping & update_parent_child_mapping)
* `handler_count` containing `consumer` and `result` (success, dropped, doclib_doc_exception and unknown_error)
* `file_operation_latency` containing `source`, `target`, `size` and `operation` (move, remove or copy)

## Testing
```bash
docker-compose up -d
sbt clean test it:test
```
