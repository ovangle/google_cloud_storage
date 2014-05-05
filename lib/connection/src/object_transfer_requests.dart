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


  Future<StorageObject> resumableUpload(
      String bucket,
      var /* String | StorageObject */ object,
      Source source,
      { int ifGenerationMatch,
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
          ..['X-Upload-Content-Type'] = source.contentType.toString()
          ..['X-Upload-Content-Length'] = source.length.toString();

      var query = new _Query(projectId)
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['uploadType'] = 'resumable';

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
          _resumeUploadAt(location, source, 0);
        } else {
          _uploadChunked(location, source);
        }
      });
    });
  }


  /**
   * A resumable upload which completes with a single request
   * (if no errors are encountered), thus completing with minimal
   * network costs.
   *
   * It can only be used with [SearchableSource] objects.
   */
  Future<StorageObject> _resumeUploadAt(
      Uri uploadUri,
      SearchableSource source,
      int position) {
    source.setPosition(position);
    http.StreamedRequest request = new http.StreamedRequest("PUT", uploadUri);


    var contentRange = new ContentRange(new Range(position, source.length - 1), source.length);
    request.headers[HttpHeaders.CONTENT_LENGTH] = (contentRange.length - contentRange.range.lo).toString();
    request.headers[HttpHeaders.CONTENT_TYPE] = source.contentType.toString();
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
        .then(_handleStorageObjectResponse('*'))
        .catchError((err, stackTrace) {
          var rpcError = (err as RPCException);
          if (_RETRY_STATUS.contains(rpcError.statusCode)) {
            return _getUploadStatus(uploadUri, source)
                .then((range) {
              if (range.hi >= source.length) {
                //Request has already been completed
                //FIXME: We should return a getStorageObject response here.
                return null;
              } else {
                return _resumeUploadAt(uploadUri, source, range.hi + 1);
              }
            });
          }
          throw err;
        }, test: (err) => err is RPCException);
  }

  Future<StorageObject> _uploadChunked(
      Uri uploadUri,
      Source source) {
    throw new UnimplementedError('ObjectTransferRequests._uploadChunked');
  }

  Future<Range> _getUploadStatus(Uri uploadUri, Source source) {
    return new Future.sync(() {
      var contentRange = new ContentRange(null, source.length);
      http.Request request = new http.Request("PUT", uploadUri)
          ..headers[HttpHeaders.CONTENT_RANGE] = contentRange.toString();
      return _sendAuthorisedRequest(request)
          .then(http.Response.fromStream)
          .then(_handleResumableUploadStatus(source));
    });
  }

  _ResponseHandler _handleResumableUploadStatus(Source source) {
    return (http.Response response) {
      _handleResponse(response)
        .then((response) {
          if (response.statusCode == HttpStatus.OK || response.statusCode == HttpStatus.CREATED) {
            return new Range(0, source.length);
          }
          if (response.statusCode == HttpStatus.PARTIAL_CONTENT) {
            var range = response.headers[HttpHeaders.RANGE];
            if (range == null)
              throw new RPCException.noRangeHeader(response);
            return Range.parse(range);
          }
          throw new RPCException.invalidStatus(response);
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

  ContentType get contentType;

  /**
   * Create a new [Source] from the specified [:file:]
   */
  static Future<Source> fromFile(File file, String contentType, { void onError(err, [StackTrace stackTrace]) }) =>
      file.open(mode: FileMode.READ)
      .then((f) => new _FileSource(f, ContentType.parse(contentType), onError: onError));

  static Future<Source> fromStream(Stream<List<int>> stream, int contentLength, String contentType, {void onError(err, [StackTrace stackTrace])}) =>
      new Future.value(new _StreamSource(stream, contentLength, ContentType.parse(contentType), onError: onError));

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

  final ContentType contentType;
  final RandomAccessFile _file;

  final Function onError;

  bool _started = false;
  int _fileLength;
  int _filePos;

  _FileSource(this._file, this.contentType, {this.onError});

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

  int _chunkPos;
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
    },
    onError: (err, stackTrace) {
      if (this.onError != null)
        onError(err,stackTrace);
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
      return new Future.delayed(new Duration(seconds: 3), this.current);
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