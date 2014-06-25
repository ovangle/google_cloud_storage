part of connection;

/**
 * The number of bytes to fetch from the buffer at each access.
 */
const int _BUFFER_SIZE = 5 * 1024 * 1024;

class _StatusResponse {
  ResumeToken token;
  RpcResponse response;

  _StatusResponse(this.token, this.response);
}

abstract class ObjectTransferRequests implements ObjectRequests {

  Stream<List<int>> downloadObject(String bucket, String object, { Map<String, String> queryParams }) {

    object = _urlEncode(object);
    StreamController controller = new StreamController<List<int>>();

    //Set the upload type to 'resumable'
    var query = new _Query();

    var uploadRpc = new RpcRequest("/b/$bucket/o/$object", headers: { HttpHeaders.RANGE: range.toString() },
        query: queryParams);

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
   * [:queryParams:] is a map of items that will be appended as query parameters. Can be used to any of the parameters
   * needed for correctly using the service.
   *
   * Returns a [Future] that completes with [ResumeToken]. This resume token can be passed directly into
   * `resumeUpload` to begin uploading the [Source].
   */
  Future<ResumeToken> uploadObject(String bucket, var /* String | StorageObject */ object, String mimeType,
                                   Source source, { Map<String, String> queryParams }) {
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
      if (queryParams == null) queryParams = {};
      queryParams['uploadType'] = 'resumable';

      var uploadRpc = new RpcRequest(
          "/b/$bucket/o",
          method: "POST",
          query: queryParams,
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

        Completer rpcRequestCompleter = new Completer();
        StreamedRpcRequest rpcRequest = new StreamedRpcRequest(Uri.parse(location), method: 'PUT');
        rpcRequest.headers.putIfAbsent('Content-Type', () => mimeType);
        rpcRequest.addSource(source);
        _client.send(rpcRequest)
        .then((RpcResponse resp) => rpcRequestCompleter.complete(resp))
        .catchError((e) => rpcRequestCompleter.completeError(e));


        return new ResumeToken(
            Uri.parse(location),
            selector: queryParams['fields'],
            done: rpcRequestCompleter.future
        );
      });
    });
  }

  /**
   * Check the status of a partially uploaded [Source]. The argument must be the resume token initially
   * returned by `uploadObject`.
   *
   * Returns a [ResumeToken] which can be used to resume the uploaded with the remainder of the source.
    */
  Future<_StatusResponse> _getUploadStatus(ResumeToken resumeToken, Source source) {
    return new Future.sync(() {

      var contentRange = new ContentRange(null, source.length);

      RpcRequest request = new RpcRequest(resumeToken.uploadUri,method: "PUT")
          ..headers['content-range'] = contentRange.toString();

      return _client.send(request).then((response) {
        if (response.statusCode == HttpStatus.OK ||
            response.statusCode == HttpStatus.CREATED) {
          return new ResumeToken.fromToken(resumeToken);
        }

        if (response.statusCode == HttpStatus.PARTIAL_CONTENT ||
            response.statusCode == 308 /* Resume Incomplete */) {
          if (response.headers.containsKey('range')) {
            var range = response.headers['range'];
            return new _StatusResponse(new ResumeToken.fromToken(resumeToken, range: Range.parse(range)), response);
          } else {
            return new _StatusResponse(new ResumeToken.fromToken(resumeToken), response);
          }

        }

        throw new RpcException.invalidStatus(response);
      });
    });
  }

  Future<StorageObject> resumeUpload(ResumeToken resumeToken, Source source) {
    return _getUploadStatus(resumeToken, source).then((_StatusResponse statusResponse) {
      return _handleResumeResponse(statusResponse.response, statusResponse.token, source);
    });
  }

  Future<RpcResponse> _handleResumeResponse(RpcResponse response, ResumeToken token, Source source) {
    if (response.statusCode == _RESUME_INCOMPLETE_STATUS) {
      var rangeToUpload = (token.range != null  ? new Range(token.range.hi + 1, source.length - 1) : new Range(0, source.length -1));
      var contentRange = new ContentRange(rangeToUpload, source.length);

      var request = new StreamedRpcRequest(token.uploadUri, method: "PUT")
        ..headers['content-range'] = contentRange.toString()
        ..addSource(source, rangeToUpload.lo);

      return _client.send(request, retryRequest: false).then((RpcResponse response) => _handleResumeResponse(response, token, source));
    } else if (_RETRY_STATUS.contains(response.statusCode)) {
      return resumeUpload(token, source);
    } else if ([HttpStatus.OK, HttpStatus.CREATED].contains(response.statusCode)) {
      return new StorageObject.fromJson(response.jsonBody, selector: token.selector);
    } else {
      throw new RpcException.invalidStatus(response);
    }
  }

}
