part of connection;




abstract class ObjectTransferRequests implements ConnectionBase {

  /**
   * Store a new [:object:] with the given [:mimeType:] in the given [:bucket:],
   * overwriting the old one if one exists. This method is suitable for small
   * objects (with a size <= `5MB`), as it retries the upload completely on
   * failure. For resumableUploads, use the [:uploadObjectResumable:] method.
   *
   * [:object:] must be either a [String] or [StorageObject]. If a [String],
   * then default values for the object metadata versions will be provided by
   * server.
   *
   * [:ifGenerationMatch:] makes the operation's success dependent on the object if it's [:generation:]
   * matches the provided value.
   * [:ifGenerationNotMatch:] makes the operation's success dependent if it's [:generation:]
   * does not match the provided value.
   * [:ifMetagenerationMatch:] makes the operation's success dependent if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] makes the operation's success dependent if its [:metageneration:]
   * does not match the provided value.
   *
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the [:bucket:].
   *
   * [:predefinedAcl:] is a [PredefinedAcl] to apply to the object. Default is [PredefinedAcl.PRIVATE]..
   *
   * Returns a [Future] which completes with the metadata of the uploaded object,
   * with fields populated by the given [:selector:].
   */
  Future<StorageObject> uploadObject(
      String bucket,
      var /* String | StorageObject */ object,
      String mimeType,
      List<int> uploadData,
      { int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        PredefinedAcl predefinedAcl: PredefinedAcl.PRIVATE,
        String projection: 'noAcl',
        String selector: '*'
      }) {
    return new Future.sync(() {
      if (object is String) {
        object = new StorageObject(bucket, object, selector: selector);
      } else if (object is! StorageBucket) {
        throw new ArgumentError("Expected a String or StorageObject");
      }

      var query = new _Query(projectId)
          ..['uploadType'] = 'multipart'
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['projection'] = projection
          ..['predefinedAcl'] = predefinedAcl
          ..['fields'] = selector;

      Map<String,String> headers = new Map<String,String>()
          ..[HttpHeaders.CONTENT_TYPE] = _MULTIPART_CONTENT;

      var metadataContent = new _MultipartRequestContent()
          ..headers[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT
          ..body = UTF8.encode(JSON.encode(object));

      var md5Hash = CryptoUtils.bytesToBase64((new MD5()..add(uploadData)).close());

      var uploadContent = new _MultipartRequestContent()
          ..headers[HttpHeaders.CONTENT_TYPE] = mimeType
          //..headers[HttpHeaders.CONTENT_LENGTH] = '${uploadData.length}'
          ..body = uploadData;

      logger.info("Uploading $object as multipart request");
      logger.info("Mime type: $mimeType");

      return _remoteProcedureCall(
          "/b/$bucket/o",
          method: "POST",
          query: query,
          headers: headers,
          body: [metadataContent, uploadContent],
          isUploadUrl: true,
          handler: _handleStorageObjectResponse(selector));

    });
  }

  /**
   * Download the object.
   */
  Stream<List<int>> downloadObject(
      String bucket,
      String object,
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch
      }) {
      var query = new _Query(projectId)
          ..['generation'] = generation
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['alt'] = 'media';
      object = _urlEncode(object);

      var url = _platformUrl("/b/$bucket/o/$object", query);

      StreamController<List<int>> controller = new StreamController<List<int>>();

      var request = new http.Request("GET", url);

      _sendAuthorisedRequest(request)
          .then(_handleResponse)
          .then((http.StreamedResponse response) =>
            controller.addStream(response.stream)
            .then((_) => controller.close())
          )
          .catchError(controller.addError);

      return controller.stream;
  }

  Stream<List<int>> downloadObjectResumable(
       String bucket,
       String object,
       { int generation,
         int ifGenerationMatch,
         int ifGenerationNotMatch,
         int ifMetagenerationMatch,
         int ifMetagenerationNotMatch,
         Range byteRange
       }) {
       var query = new _Query(projectId)
           ..['generation'] = generation
           ..['ifGenerationMatch'] = ifGenerationMatch
           ..['ifGenerationNotMatch'] = ifMetagenerationMatch
           ..['ifMetagenerationMatch'] = ifMetagenerationMatch
           ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
           ..['alt'] = 'media';
       object = _urlEncode(object);

       var url = _platformUrl("/b/$bucket/o/$object", query);

       var request = new http.Request("GET", url);

       if (byteRange != null) {
         request.headers[HttpHeaders.RANGE] = byteRange.toString();
       }

       StreamController<List<int>> controller = new StreamController<List<int>>();

       _sendAuthorisedRequest(request)
           .then(_handleResponse)
           .then((http.StreamedResponse response) {
             var contentLength = response.contentLength;
             int byteCounter = 0;
             response.stream.listen(
                 (List<int> data) {
                    controller.add(data);
                    byteCounter += data.length;
                  },
                  onError: (err, stackTrace) {
                    logger.warning("Encountered error when reading response stream\n"
                        "Resuming", err, stackTrace);
                    Range range = new Range(byteCounter + 1, contentLength - 1);
                    return downloadObjectResumable(
                        bucket,
                        object,
                        generation: generation,
                        ifGenerationMatch: ifGenerationMatch,
                        ifGenerationNotMatch: ifGenerationNotMatch,
                        ifMetagenerationMatch: ifMetagenerationMatch,
                        ifMetagenerationNotMatch: ifMetagenerationNotMatch,
                        byteRange: range);
                  },
                  onDone: () {
                    controller.close();
                  });
            })
           .catchError(controller.addError);

       return controller.stream;
   }

  /**
   * Store a new [:object:] with the given [:mimeType:] to the specified [:bucket:],
   * overwriting any file which already exists with the given name.
   * This method is suitable for any size of object, as it automatically resumes
   * the download at the last uploaded byte when the download fails.
   *
   * Currently the method only supports uploading a [File] type object.
   *
   * [:object:] must be either a [String] or [StorageObject]. If a [String],
   * then default values for the object metadata versions will be provided by
   * server.
   *
   * [:ifGenerationMatch:] makes the operation's success dependent on the object if it's [:generation:]
   * matches the provided value.
   * [:ifGenerationNotMatch:] makes the operation's success dependent if it's [:generation:]
   * does not match the provided value.
   * [:ifMetagenerationMatch:] makes the operation's success dependent if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] makes the operation's success dependent if its [:metageneration:]
   * does not match the provided value.
   *
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the [:bucket:].
   *
   * [:predefinedAcl:] is a [PredefinedAcl] to apply to the object. Default is [PredefinedAcl.PRIVATE]..
   *
   * Returns a [Future] which completes with the metadata of the uploaded object,
   * with fields populated by the given [:selector:].
   */
  Future<StorageObject> uploadObjectResumable(
      String bucket,
      var /* String | StorageObject */ object,
      String mimeType,
      Source source,
      { int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        PredefinedAcl predefinedAcl: PredefinedAcl.PRIVATE,
        String projection: 'noAcl',
        String selector: '*'
      }) {
    return new Future.sync(() {
      if (object is String) {
        object = new StorageObject(bucket, object, selector: selector);
      } else if (object is! StorageObject) {
        throw new ArgumentError("Expected String or StorageObject");
      }

      var headers = new Map()
          ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT
          ..['X-Upload-Content-Type'] = mimeType
          ..['X-Upload-Content-Length'] = source.length.toString();

      var query = new _Query(projectId)
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['uploadType'] = 'resumable';

      _ResponseHandler handler = _handleStorageObjectResponse(selector);

      return _remoteProcedureCall(
          "/b/$bucket/o",
          method: "POST",
          headers: headers,
          query: query,
          body: object,
          isUploadUrl: true,
          handler: _handleResumableUploadInit)
      .then((location) {
        if (source is SearchableSource) {
          return _resumeUploadAt(
              location,
              mimeType,
              source,
              0,
              handler);
        } else {
          throw new UnimplementedError("Only searchable sources implemented");
          return _uploadChunked(location, source);
        }
      });
    });
  }


  /**
   * A resumable upload which completes with a single request
   * (if no errors are encountered), thus completing with minimal
   * network costs.
   *
   * It can only be used with [SearchableSource] objects (ie. Files)
   */
  Future<StorageObject> _resumeUploadAt(
      Uri uploadUri,
      String contentType,
      SearchableSource source,
      int position,
      _ResponseHandler metadataHandler) {
    source.setPosition(position);
    http.StreamedRequest request = new http.StreamedRequest("PUT", uploadUri);

    var contentRange = new ContentRange(new Range(position, source.length - 1), source.length);
    request.headers[HttpHeaders.CONTENT_LENGTH] = (contentRange.length - contentRange.range.lo).toString();
    request.headers[HttpHeaders.CONTENT_TYPE] = contentType;
    request.headers[HttpHeaders.CONTENT_RANGE] = contentRange.toString();

    print(request.headers);

    var uploadId = request.url.queryParameters['upload_id'];
    logger.info("Resuming object upload (uploadId: $uploadId)");
    logger.info("Content length: ${source.length}");
    logger.info("Content range: $contentRange");

    int pos = 0;

    //Add the next chunk to the stream
    addNextChunk() {
      if (!source.moveNext()) {
        assert(pos == source.length);
        return request.sink.close();
      }
      return source.current()
          .then((bytes) {
            logger.info("Adding byte range ${pos}-${pos + bytes.length} to body of upload $uploadId");
            pos += bytes.length;
            request.sink.add(bytes);
            return addNextChunk();
          });
    }

    addNextChunk();

    return _sendAuthorisedRequest(request)
        .then(http.Response.fromStream)
        .then(metadataHandler)
        .catchError((err, stackTrace) {
          var rpcError = (err as RPCException);
          if (_RETRY_STATUS.contains(rpcError.statusCode)) {
            return _getUploadStatus(uploadUri, source, metadataHandler)
                .then((result) {
              if (result is StorageObject) {
                return range;
              } else if (result is Range) {
                return _resumeUploadAt(uploadUri, contentType, source, result.hi + 1, metadataHandler);
              }
            });
          }
          throw err;
        }, test: (err) => err is RPCException);
  }
  /**
   * A resumable upload which can be used with any [Source] type, but which emits a
   * request for each block of data in the source.
   *
   * Slower as it uses more network requests to complete the upload, but can be
   * used with [Sources] which provide data asyncronously (eg. [Stream]s)
   */
  Future<StorageObject> _uploadChunked(
      Uri uploadUri,
      Source source) {
    throw new UnimplementedError("ObjectTransferRequests.uploadChunked");

    /*
    var uploadId = uploadUri.queryParameters['upload_id'];
    logger.info("Resuming object upload (uploadId: $uploadId)");
    logger.info("Content length: ${source.length}");

    Future sendNextBlock() {

      Future sendCurrentBlock() {
        return source.current().then((block) {

          var sourcePos = source.currentPosition;
          var range = new ContentRange(
              new Range(sourcePos, sourcePos + block.length - 1),
              source.length);
          http.Request request = new http.Request("PUT", uploadUri)
              ..headers[HttpHeaders.CONTENT_TYPE] = mimeType
              ..headers[HttpHeaders.CONTENT_RANGE] = range.toString();

          logger.info("Sending chunk with ${range}\n"
                      "\tto upload $uploadId");
          print(block.where((v) => v == null));
          request.bodyBytes = new Uint8List.fromList(block);

          return _sendAuthorisedRequest(request)
              .then(http.Response.fromStream)
              .then(_handleResumableUploadStatus(source))
              .then((result) {
                if (result is StorageObject)
                  return result;
                if (result is Range) {
                  //We haven't added the whole range for the block.
                  //Resend it.
                  if (result.hi != range.range.hi) {
                    logger.warning("Retrying upload chunk\n"
                                   "\tUpload: $uploadId\n"
                                   "\tReason Result range ($result) does not match request range ($range)");
                    return sendCurrentBlock();
                  }
                  return sendNextBlock();
                }
              });
        });
      }

      if (!source.moveNext()) {
        return new Future.value();
      }
      return sendCurrentBlock();
    }

    return sendNextBlock();
    */

  }

  Future<Range> _getUploadStatus(Uri uploadUri, Source source, metadataHandler) {
    return new Future.sync(() {
      var contentRange = new ContentRange(null, source.length);
      http.Request request = new http.Request("PUT", uploadUri)
          ..headers[HttpHeaders.CONTENT_RANGE] = contentRange.toString();
      return _sendAuthorisedRequest(request)
          .then(http.Response.fromStream)
          .then(_handleResumableUploadStatus(source, metadataHandler));
    });
  }

  /**
   * Check the response status of a partial resume upload request
   * If the status is one of
   * - 200 OK
   * - 201 CREATED
   * Then the response contains the object metadata and the body is parsed as if it
   * contains the object metadata
   * If the status is one of
   * - 206 PARTIAL CONTENT
   * - 308 RESUME INCOMPLETE
   * Then the response is considered to be a resume status and the `Range` header
   * is parsed as a [Range].
   *
   * Otherwise, response is redirected to [:_handleResponse:]
   */
  _ResponseHandler _handleResumableUploadStatus(Source source, _ResponseHandler metadataHandler) {
    return (http.Response response) {
      return new Future.sync(() {
        if (response.statusCode == HttpStatus.OK || response.statusCode == HttpStatus.CREATED) {
          try {
            return metadataHandler(response);
          } on RPCException catch (e) {
            logger.severe(e.toString());
            throw e;
          }
        }
        if (response.statusCode == HttpStatus.PARTIAL_CONTENT || response.statusCode == 308 /* Resume incomplete */) {
          var range = response.headers[HttpHeaders.RANGE];
          if (range == null)
            throw new RPCException.noRangeHeader(response);
          return Range.parse(range);
        }
        return _handleResponse(response);
      });
    };
  }

  Future<Uri> _handleResumableUploadInit(http.Response response) {
    if (response.statusCode != HttpStatus.OK)
      throw new RPCException.invalidStatus(response);
    return new Future.value(Uri.parse(response.headers[HttpHeaders.LOCATION]));
  }

  _ResponseHandler _handleStorageObjectResponse(String selector) {
    return (http.Response response) =>
        _handleJsonResponse(response)
        .then((result) => new StorageObject.fromJson(result, selector: selector));
  }
}

/**
 * An interface which represents a generic searchable,
 * readable object.
 *
 * A [Source] always has a valid [:contentType:]
 */
abstract class Source {

  /**
   * The size of each chunk to read from the [Source].
   */
  static const CHUNK_SIZE = 256 * 1024;

  /**
   * Create a new [Source] from the specified [:file:]
   */
  static Future<Source> fromFile(File file, { void onError(err, [StackTrace stackTrace]) }) =>
      file.open(mode: FileMode.READ)
      .then((f) => new _FileSource(f, onError: onError));

  /*
  static Source fromStream(Stream<List<int>> stream, int contentLength, String contentType, {void onError(err, [StackTrace stackTrace])}) =>
      new _StreamSource(stream, contentLength, ContentType.parse(contentType), onError: onError);
  */
  /**
   * Get the length of the [Source] in bytes
   */
  int get length;

  /**
   * The start position in the [Source] of the current chunk
   */
  int get currentPosition;

  /**
   * Get the current chunk of data from the [Source].
   * Every chunk (except the last) is expected to be a multiple of [CHUNK_SIZE]
   * in length.
   */
  Future<List<int>> current();

  /**
   * Clear the current chunk.
   * Returns `true` if this is the end of the [Source].
   */
  bool moveNext();

  /**
   * Close the source.
   */
  Future close();


  /**
   * An error handler for the source.
   */
  Function get onError;
}

/**
 * A [SearchableSource] is a [Source] which can be restarted
 * from any position in the source.
 */
abstract class SearchableSource extends Source {
  /**
   * Set the position in the [SearchableSource] at which
   * to resume the upload.
   */
  void setPosition(int position);
}

/**
 * Files are searchable and can be uploaded in a single contiguous
 * chunk.
 */
class _FileSource implements SearchableSource {
  static final CHUNK_SIZE = Source.CHUNK_SIZE;

  final RandomAccessFile _file;

  final Function onError;

  bool _started = false;
  int _fileLength;
  int _filePos;

  _FileSource(this._file, {this.onError});

  int get length {
    if (_fileLength == null)
      _fileLength = _file.lengthSync();
    return _fileLength;
  }

  int get currentPosition => _filePos != null ? math.min(_filePos, _fileLength) : null;

  Future<List<int>> current() =>
      _file.setPosition(_filePos)
      .then((file) => file.read(CHUNK_SIZE))
      .catchError(onError);

  bool moveNext() {
    if (!_started) {
      _started = true;
      return true;
    }
    _filePos += CHUNK_SIZE;
    return _filePos < length;
  }

  void setPosition(int position) {
    if (position < 0 || position >= _fileLength)
      throw new RangeError.range(position, 0, _fileLength - 1);
    _filePos = position;
  }

  Future close() => _file.close();

}

/**
 * A [_StreamSource] is a source which is dynamically generated from a [Stream].
 * Bytes are read from the stream in [CHUNK_SIZE] chunks. When the server returns
 * a status which
 */
/*
class _StreamSource implements Source {
  static const int CHUNK_SIZE = Source.CHUNK_SIZE;

  final int _length;
  final ContentType contentType;

  int _streamPos;

  /**
   * The number of chunks in the current block to be written to the upload.
   * This is also the number of chunks that are currently held in memory.
   */
  int _numChunksInCurrentBlock = 0;

  /**
   * A queue of chunks waiting to be uploaded. As each chunk of bytes is read
   * from the stream, it is added to the queue, ready to be read and uploaded
   * to the cloud storage server.
   */
  var _pendingChunks = new LinkedList<List<int>>();

  int _chunkPos = 0;
  var _currentChunk = new List<int>(CHUNK_SIZE);

  final Function onError;

  StreamSubscription<List<int>> _streamSubscription;

  _StreamSource(Stream stream, int this._length, this.contentType, {this.onError}) {
    this._streamSubscription = stream.listen((Iterable<int> data) {
      while (_chunkPos + data.length > CHUNK_SIZE) {
        _currentChunk.setRange(_chunkPos, Source.CHUNK_SIZE, data.take(CHUNK_SIZE - _chunkPos));
        data = data.skip(CHUNK_SIZE - _chunkPos);
        _pendingChunks.add(_currentChunk);
        _currentChunk = new List<int>(CHUNK_SIZE);
        _chunkPos = 0;
      }
      _currentChunk.setRange(_chunkPos, data.length, data);
      _chunkPos += data.length;
    },
    onError: (err, stackTrace) {
      if (this.onError != null) {
        onError(err,stackTrace);
        _streamSubscription.cancel();
      } else {
        print("Error occurred when handling stream");
        throw err;
      }
    },
    onDone: () {
      //Add the last partial chunk to the pending chunks.
      _pendingChunks.add(_currentChunk);
      _currentChunk = null;
      return _streamSubscription.cancel();
    });
  }

  @override
  Future close() => _streamSubscription.cancel();

  @override
  Future<List<int>> current() {
    if (_pendingChunks.isEmpty) {
      return new Future.delayed(
          new Duration(seconds: 3),
          () => this.current()
      );
    }
    return new Future.sync(() {
      _numChunksInCurrentBlock = _pendingChunks.length;

      return new List.from(
          _pendingChunks.take(_numChunksInCurrentBlock).expand((i) => i),
          growable: false
      );
    });
  }

  @override
  int get currentPosition => _streamPos != null ? math.min(_streamPos, _length) : null;

  @override
  int get length => _length;

  @override
  bool moveNext() {
    if (_streamPos == null) {
      _streamPos = 0;
      return true;
    }
    //The currently pending blocks should have been read before moving
    assert(_numChunksInCurrentBlock != 0);
    for (var i in range(_numChunksInCurrentBlock)) {
      _pendingChunks.removeFirst();
    }
    _streamPos += _numChunksInCurrentBlock * CHUNK_SIZE;
    _numChunksInCurrentBlock = null;
    return _streamPos <= _length;
  }
}
*/