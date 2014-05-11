part of connection;

abstract class ObjectTransferRequests implements ObjectRequests {

  Stream<List<int>> downloadObject(
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

     return _downloadObject(url, byteRange);
   }



  Stream<List<int>> _downloadObject(Uri path, Range range) {
    var request = new http.Request("GET", path)
        ..headers[HttpHeaders.RANGE] = range.toString();

    StreamController controller = new StreamController<List<int>>();

    _sendAuthorisedRequest(request)
        .then(_handleResponse)
        .then((http.StreamedResponse response) {
      logger.info("here");
      logger.info(response.headers.toString());
      var expectedMd5Hash = this._expectedMd5Header(response.headers);

      var contentLength = response.contentLength;

      int counter = 0;
      var md5Hash = new MD5();

      void addBytes(List<int> bytes) {
        md5Hash.add(bytes);
        controller.add(bytes);
        counter += bytes.length;
      }

      var subscription;
      subscription = response.stream.listen(
        addBytes,
        onError: (err, stackTrace) {
          logger.warning("Encountered error when reading response stream\n"
                                 "Resuming", err, stackTrace);
         Range range = new Range(counter + 1, contentLength - 1);
         _downloadObject(path, range).listen(
             addBytes,
             onError: controller.addError,
             onDone: controller.close);
         subscription.cancel();
        },
        onDone: () {
          controller.close();
          // Check the value of the hash we built while downloading the object
          // to the one provided in the header
          if (expectedMd5Hash != null) {
            var actualHash = new Uint8List.fromList(md5Hash.close());
            if (!_LIST_EQ.equals(expectedMd5Hash, actualHash)) {
              throw new ObjectTransferException("Md5 hash mismatch. Retry download");
            }
          }
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
   * [:predefinedAcl:] is a [PredefinedAcl] to apply to the object. Default is [PredefinedAcl.PROJECT_PRIVATE]..
   *
   * Returns a [Future] which completes with the metadata of the uploaded object,
   * with fields populated by the given [:selector:].
   */
  Future<StorageObject> uploadObject(
      String bucket,
      var /* String | StorageObject */ object,
      String mimeType,
      Source source,
      { int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        PredefinedAcl predefinedAcl: PredefinedAcl.PROJECT_PRIVATE,
        String projection: 'noAcl',
        String selector: '*'
      }) {
    return source.md5().then((hash) {
      if (object is String) {
        object = new StorageObject(bucket, object, selector: selector);
      } else if (object is! StorageObject) {
        throw new ArgumentError("Expected String or StorageObject");
      }

      var headers = new Map()
          ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT
          ..['X-Upload-Content-Type'] = mimeType
          ..['X-Upload-Content-Length'] = source.length.toString()
          ..['X-Upload-Content-Md5'] = CryptoUtils.bytesToBase64(hash);

      var query = new _Query(projectId)
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['predefinedAcl'] = predefinedAcl
          ..['projection'] = projection
          ..['uploadType'] = 'resumable';

      _ResponseHandler handler = _handleStorageObjectResponse(selector);

      return _remoteProcedureCall(
          "/b/$bucket/o",
          method: "POST",
          headers: headers,
          query: query,
          body: object,
          isUploadUrl: true,
          handler: _handleResumableUploadInit
      ).then((location) {
        var range = new ContentRange(new Range(0, source.length - 1), source.length);
        return _resumeUploadAt(location, mimeType, source, hash, range, handler);
      });
    });
  }

  /**
   * The number of bytes to fetch from the buffer at each access.
   */
  static const int _BUFFER_SIZE = 256 * 1024;


  /**
   * A resumable upload which completes with a single request
   * (if no errors are encountered), thus completing with minimal
   * network costs.
   */
  Future<StorageObject> _resumeUploadAt(
      Uri uploadUri,
      String contentType,
      Source source,
      List<int> sourceMd5,
      ContentRange contentRange,
      _ResponseHandler metadataHandler) {
    source.setPosition(contentRange.range.lo);
    http.StreamedRequest request = new http.StreamedRequest("PUT", uploadUri)
        ..headers['content-length'] = (contentRange.length - contentRange.range.lo).toString()
        ..headers['content-type'] = contentType
        ..headers['content-range'] = contentRange.toString();
    if (sourceMd5 != null)
      request.headers['content-md5'] = CryptoUtils.bytesToBase64(sourceMd5);

    print(request.headers);

    var uploadId = request.url.queryParameters['upload_id'];
    logger.info("Resuming object upload (uploadId: $uploadId)");
    logger.info("Content length: ${source.length}");
    logger.info("Content range: $contentRange");


    //Add the next chunk to the stream.
    //Seperate the source into chunks of size [_BUFFER_SIZE] to avoid
    //loading the whole source into memory at once.
    addChunkAt(int pos) {
      if (pos >= source.length) return request.sink.close();
      source.setPosition(pos);
      return source.read(_BUFFER_SIZE)
          .then((bytes) {
              request.sink.add(bytes);
              return addChunkAt(0 + _BUFFER_SIZE);
          });
    }

    addChunkAt(0);

    resume(ContentRange range) => _resumeUploadAt(uploadUri, contentType, source, sourceMd5, range, metadataHandler);

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
                var resumeRange = new ContentRange(
                    new Range(result.hi + 1, source.length - 1), source.length);
                return resume(resumeRange);
              }
            });
          }
          throw err;
        }, test: (err) => err is RPCException);
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

  static final Pattern _GOOG_HASH_HEADER = new RegExp(r'md5=(.*)');

  Uint8List _expectedMd5Header(Map<String,String> responseHeaders) {
    var googHash = responseHeaders['x-goog-hash'];
    if (googHash == null) return null;
    googHash = googHash.split(',');
    for (var hash in googHash) {
      if (hash.startsWith('md5=')) {
        return new Uint8List.fromList(
            CryptoUtils.base64StringToBytes(hash.substring('md5='.length))
        );
      }
    }
    return null;
  }
}