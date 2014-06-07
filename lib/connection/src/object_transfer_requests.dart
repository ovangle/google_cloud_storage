part of connection;

/**
 * The number of bytes to fetch from the buffer at each access.
 */
const int _BUFFER_SIZE = 5 * 1024 * 1024;

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

  Stream<List<int>> _downloadObject(Uri url, Range range) {
    var rpc = new _RemoteProcedureCall(url, "GET",
        headers: { HttpHeaders.RANGE: range.toString() }
    );

    StreamController controller = new StreamController<List<int>>();

    _sendAuthorisedRequest(rpc.asRequest())
        .then((http.StreamedResponse response) {
      var expectedMd5Hash = _parseMd5Header(response.headers);

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
         _downloadObject(url, range).listen(
             addBytes,
             onError: controller.addError,
             onDone: controller.close);
         subscription.cancel();
        },
        onDone: () {
          controller.close();
          // Compare the value of the hash we built while downloading the object
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
   * Returns a [Future] that completes with [ResumeToken]. This resume token can be passed directly into
   * `resumeUpload` to begin uploading the [Source].
   */
  Future<ResumeToken> uploadObject(
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
    return source.md5().then((contentMd5) {
      if (object is String) {
        object = new StorageObject(bucket, object);
      } else if (object is! StorageObject) {
        throw new ArgumentError('Expected a `String` or `StorageObject`');
      }

      var headers = new Map()
          ..['X-UploadContent-Type'] = mimeType
          ..['X-Upload-Content-Length'] = source.length.toString()
          ..['X-Upload-Content-MD5'] = CryptoUtils.bytesToBase64(contentMd5)
          ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      var query = new _Query(projectId)
                ..['ifGenerationMatch'] = ifGenerationMatch
                ..['ifGenerationNotMatch'] = ifGenerationNotMatch
                ..['ifMetagenerationMatch'] = ifMetagenerationMatch
                ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
                ..['predefinedAcl'] = predefinedAcl
                ..['projection'] = projection
                ..['fields'] = selector;

      var getObjectRpc = new _RemoteProcedureCall(
          _platformUrl("/b/$bucket/o/${(object as StorageObject).name}", query),
          "GET");

      query['uploadType'] = 'resumable';

      return _remoteProcedureCall(
          "/b/$bucket/o",
          method: "POST",
          headers: headers,
          query: query,
          body: object,
          isUploadUrl: true,
          handler: _handleUploadInitResponse
      ).then((uri) {
        return new ResumeToken(ResumeToken.TOKEN_INIT, uri, new Range(0, -1), getObjectRpc, selector);
      });
    });
  }

  /**
   * Check the status of a partially uploaded [Source]. The argument must be the resume token initially
   * returned by `uploadObject`.
   *
   * Returns a [ResumeToken] which can be used to resume the uploaded with the remainder of the source.
   *
   * It is important to check whether [:resumeToken.isCompleted:] after retrieving the
   * current upload status as it is possible that the connection was interrupted after the
   * server received the last byte but before the response was sent.
    */
  Future<ResumeToken> getUploadStatus(ResumeToken resumeToken, Source source) {
    return new Future.sync(() {

      var contentRange = new ContentRange(null, source.length);
      http.Request request = new http.Request("PUT", resumeToken.uploadUri)
          ..headers[HttpHeaders.CONTENT_RANGE] = contentRange.toString();
      return _sendAuthorisedRequest(request)
          .then(http.Response.fromStream)
          .then((http.Response response) {
            if (response.statusCode == HttpStatus.OK ||
                response.statusCode == HttpStatus.CREATED) {
              var uploadedRange = new Range(0, source.length - 1);
              return new ResumeToken._from(resumeToken, ResumeToken.TOKEN_COMPLETE, uploadedRange);
            }

            if (response.statusCode == HttpStatus.PARTIAL_CONTENT ||
                response.statusCode == 308 /* Resume Incomplete */) {
              var range = response.headers[HttpHeaders.RANGE];
              if (range == null) throw new RPCException.noRangeHeader(response);
              return new ResumeToken._from(resumeToken, ResumeToken.TOKEN_INTERRUPTED, Range.parse(range));
            }

            throw new RPCException.invalidStatus(response);
          });
    });
  }

  Future<StorageObject> resumeUpload(ResumeToken resumeToken, Source source) {
    return new Future.sync(() {
      if (resumeToken.isComplete)
        throw new StateError('Upload already complete');

      var uploadId = resumeToken.uploadUri.queryParameters['upload_id'];
      logger.info("Resuming upload $uploadId");
      logger.info("At byte: ${resumeToken.range.hi + 1}");
      logger.info("Bytes remaining: ${source.length - resumeToken.range.hi}");

      http.StreamedRequest request = new http.StreamedRequest("PUT", resumeToken.uploadUri);

      var uploadRange = new ContentRange(
          new Range(resumeToken.range.hi, source.length - 1),
          source.length
      );

      request.headers[HttpHeaders.CONTENT_RANGE] = uploadRange.toString();

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

      addChunkAt(uploadRange.range.lo);

      _sendAuthorisedRequest(request).then((response) {


        //Set up the handler for the object metadata.
        var handler = _handleStorageObjectResponse(resumeToken._selector);

        if (_RETRY_STATUS.contains(response.statusCode)) {

          return getUploadStatus(resumeToken, source).then((resumeToken) {
            if (resumeToken.isComplete) {
              //Send the (stored) request to get the object metadata.
              return _sendAuthorisedRequest(resumeToken._rpc.asRequest()).then(handler);
            } else {
              //Othwerise we still have bytes to upload. Resume the upload.
              return resumeUpload(resumeToken, source);
            }
          });
        } else if (response.statusCode == HttpStatus.OK
            || response.statusCode == HttpStatus.CREATED) {
          return handler(response);
        } else {
          throw new RPCException.invalidStatus(response);
        }
      });
    });
  }

  /**
   * Handles the response obtained when sending object metadata to begin a upload request.
   */
  Future<Uri> _handleUploadInitResponse(_RemoteProcedureCall rpc, http.BaseResponse response) {
    return _handleResponse(rpc, response).then((response) {

      if (response.statusCode != HttpStatus.OK)
        throw new RPCException.invalidStatus(response);

      var location = response.headers[HttpHeaders.LOCATION];
      if (location == null)
        throw new RPCException.noLocationHeader(response);

      return new Future.value(Uri.parse(location));
    });
  }

}

Uint8List _parseMd5Header(Map<String,String> responseHeaders) {
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

/**
 * A token which can be used to resume an upload from the point in the source where it failed.
 */
class ResumeToken {
  /**
   * The token obtained from the `uploadObject` method
   */
  static const TOKEN_INIT = 0;
  /**
   * The token obtained when a upload request was interrupted
   */
  static const TOKEN_INTERRUPTED = 1;
  /**
   * A token obtained when the upload is complete.
   */
  static const TOKEN_COMPLETE = 2;

  /**
   * The type of the token. One of [TOKEN_INIT], [TOKEN_INTERRUPTED] or [TOKEN_COMPLETE]
   */
  final int type;

  bool get isInit => type == TOKEN_INIT;
  bool get isComplete => type == TOKEN_COMPLETE;

  /**
   * The endpoint of the upload service
   */
  final Uri uploadUri;

  /**
   * The range of bytes that have been sucessfully uploaded.
   * *Note*: The specified range is a closed range, inclusive of the first and last byte positions
   * in accordance with `W3C` specifications.
   */
  final Range range;

  /**
   * A rpc which can be used to get the status of the object rather than failing
   */
  final _RemoteProcedureCall _rpc;

  /**
   * The selector of the completed upload metadata
   */
  final String _selector;

  ResumeToken(this.type, this.uploadUri, this.range, this._rpc, this._selector);

  ResumeToken._from(ResumeToken token, this.type, this.range):
    this.uploadUri = token.uploadUri,
    this._rpc = token._rpc,
    this._selector = token._selector;

  ResumeToken.fromJson(Map<String,dynamic> json):
    this(
        json['type'],
        Uri.parse(json['url']),
        Range.parse(json['range']),
        new _RemoteProcedureCall.fromJson(json['rpc']),
        json['selector']
    );

  toJson() =>
      { 'type': type,
         'url': uploadUri.toString(),
         'range': range.toString(),
         'rpc': _rpc.toJson(),
         'selector': _selector
      };
}