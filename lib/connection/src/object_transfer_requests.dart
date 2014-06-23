part of connection;

/**
 * The number of bytes to fetch from the buffer at each access.
 */
const int _BUFFER_SIZE = 5 * 1024 * 1024;

abstract class ObjectTransferRequests implements ObjectRequests {

  Stream<List<int>> downloadObject(String bucket, String object, { int ifGenerationMatch, int ifGenerationNotMatch,
      int ifMetagenerationMatch, int ifMetagenerationNotMatch, String projection, String selector }) {

    object = _urlEncode(object);
    StreamController controller = new StreamController<List<int>>();

    //Set the upload type to 'resumable'
    var query = new _Query();

    notNull(ifGenerationMatch, () => query['ifGenerationMatch'] = ifGenerationMatch);
    notNull(ifGenerationNotMatch, () => query['ifGenerationNotMatch'] = ifGenerationNotMatch);
    notNull(ifMetagenerationMatch, () => query['ifMetagenerationMatch'] = ifMetagenerationMatch);
    notNull(ifMetagenerationNotMatch, () => query['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch);
    notNull(projection, () => query['projection'] = projection);
    notNull(selector, () => query['field'] = selector);

    var uploadRpc = new RpcRequest("/b/$bucket/o/$object", headers: { HttpHeaders.RANGE: range.toString() },
        query: query);

    _client.send(uploadRpc).then((RpcResponse resp) {
      String link = JSON.decode(resp.body)['mediaLink'];
      _client.send(new RpcRequest(Uri.parse(link))).then((RpcResponse resp) {
        controller.add(resp.body);
        controller.close();
      });
    });

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
   * [:source:] is a [Source] containing a readable object.
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
        PredefinedAcl predefinedAcl,
        String projection,
        String selector
      }) {
    return new Future.sync(() {
      if (object is String) {
        object = new StorageObject(bucket, object);
      } else if (object is! StorageObject) {
        throw new ArgumentError('Expected a `String` or `StorageObject`');
      }

      var headers = new Map()
          ..['X-Upload-Content-Type'] = mimeType
          ..['X-Upload-Content-Length'] = source.length.toString()
          ..['Content-Type'] = 'application/json; charset=utf-8';

      //Set the upload type to 'resumable'
      var query = new _Query()
        ..['uploadType'] = 'resumable';

      notNull(ifGenerationMatch, () => query['ifGenerationMatch'] = ifGenerationMatch);
      notNull(ifGenerationNotMatch, () => query['ifGenerationNotMatch'] = ifGenerationNotMatch);
      notNull(ifMetagenerationMatch, () => query['ifMetagenerationMatch'] = ifMetagenerationMatch);
      notNull(ifMetagenerationNotMatch, () => query['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch);
      notNull(projection, () => query['projection'] = projection);
      notNull(selector, () => query['fields'] = selector);

      var uploadRpc = new RpcRequest(
          "/b/$bucket/o",
          method: "POST",
          query: query,
          isUploadRequest: true);

      uploadRpc.headers.addAll(headers);
      uploadRpc.jsonBody = object.toJson();


      return _client.send(uploadRpc).then((response) {

        if (response.statusCode != HttpStatus.OK)
          throw new RpcException.invalidStatus(response);

        //The location to submit the upload is the 'location' header.
        var location = response.headers['location'];
        if (location == null)
          throw new RpcException.expectedResponseHeader('location', response);

        StreamedRpcRequest rpcRequest = new StreamedRpcRequest(Uri.parse(location), method: 'PUT');
        rpcRequest.headers.putIfAbsent('Content-Type', () => mimeType);
        source.read(source.length).then((List<int> data) {
          rpcRequest.sink.add(data);
          rpcRequest.sink.close();
          _client.send(rpcRequest).then((RpcResponse resp) {
            print('Resp body: ${resp.body}');
          });
        });

        return new ResumeToken(
            ResumeToken.TOKEN_INIT,
            Uri.parse(location),
            selector
        );
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

      RpcRequest request = new RpcRequest(resumeToken.uploadUri,method: "PUT")
          ..headers['content-range'] = contentRange.toString();

      return _client.send(request).then((response) {
        if (response.statusCode == HttpStatus.OK ||
            response.statusCode == HttpStatus.CREATED) {
          return new ResumeToken.fromToken(resumeToken, ResumeToken.TOKEN_COMPLETE, rpcResponse: response);
        }

        if (response.statusCode == HttpStatus.PARTIAL_CONTENT ||
            response.statusCode == 308 /* Resume Incomplete */) {
          if (response.headers.containsKey('range')) {
            var range = response.headers['range'];
            return new ResumeToken.fromToken(resumeToken, ResumeToken.TOKEN_INTERRUPTED, range: Range.parse(range));
          } else {
            return new ResumeToken.fromToken(resumeToken, ResumeToken.TOKEN_INTERRUPTED);
          }

        }

        throw new RpcException.invalidStatus(response);
      });
    });
  }

  Future<StorageObject> resumeUpload(ResumeToken resumeToken, Source source) {
    return new Future.sync(() {
      if (resumeToken.isComplete)
        throw new StateError('Upload already complete');

      var rangeToUpload = (resumeToken.range != null  ? new Range(resumeToken.range.hi + 1, source.length - 1) :
          new Range(0, source.length -1));

      var contentRange = new ContentRange(rangeToUpload, source.length);

      var request = new StreamedRpcRequest(resumeToken.uploadUri, method: "PUT")
          ..headers['content-range'] = contentRange.toString();

      //Add the source to the request
      request.addSource(source, rangeToUpload.lo);

      return _client.send(request, retryRequest: false).then((RpcResponse response) {
        handler(RpcResponse response) =>
            new StorageObject.fromJson(response.jsonBody, selector: resumeToken.selector);

        if (_RETRY_STATUS.contains(response.statusCode)) {
          return getUploadStatus(resumeToken, source).then((resumeToken) {
            if (resumeToken.isComplete) {
              //Use stored response to get the object metadata.
              return handler(resumeToken.rpcResponse);
            } else {
              //Otherwise we still have bytes to upload. Resume the upload.
              return resumeUpload(resumeToken, source);
            }
          });

        } else if (response.statusCode == HttpStatus.OK
            || response.statusCode == HttpStatus.CREATED) {
          return handler(response);
        } else {
          throw new RpcException.invalidStatus(response);
        }
      });
    });
  }

}
