part of connection;

/**
 * The number of bytes to fetch from the buffer at each access.
 */
const int _BUFFER_SIZE = 5 * 1024 * 1024;



abstract class ObjectTransferRequests implements ObjectRequests {

  Uri downloadUrl(String bucket, String object, {Map<String,String> params: const {}}) {

    params = new Map.from(params);
    params.putIfAbsent('alt', () => 'media');

    var uploadRpc = new RpcRequest("/b/$bucket/o/$object", query: params);
    return uploadRpc.requestUrl();
  }

  Stream<List<int>> downloadObject(String bucket, String object, { Range range: null, Map<String, String> params: const {} }) {

    object = _urlEncode(object);
    StreamController controller = new StreamController<List<int>>();

    params = new Map.from(params);
    params.putIfAbsent('alt', () => 'media');

    var headers = new Map();
    if (range != null)
      headers['content-range'] = range.toString();

    var uploadRpc = new RpcRequest("/b/$bucket/o/$object", headers: headers,
        query: params);

    _client.sendHttp(uploadRpc).then((http.StreamedResponse response) {
      return controller.addStream(response.stream);
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
   * [:source:] is a readable, searchable instance which contains the object
   * data. See [Source] for more information.
   *
   * [:params:] is a map of optional query parameters to pass to the method. Infomation
   * about the valid entries in the map can be found in the [API documenation][0].
   *
   * Returns a [Future] that completes with [ResumeToken]. This resume token can be passed directly into
   * `resumeUpload` to begin uploading the [Source].
   *
   * [0]:https://developers.google.com/storage/docs/json_api/v1/objects/insert
   */
  Future<ResumeToken> uploadObject(
      String bucket,
      var /* String | StorageObject */ object,
      Source source,
      { Map<String, String> params: const {} }) {
    return new Future.sync(() {

      if (object is String) {
        object = new StorageObject(bucket, object, selector: 'bucket,name');
      } else if (object is! StorageObject) {
        throw new ArgumentError('Expected a `String` or `StorageObject`');
      }

      var headers = new Map()
          ..['X-Upload-Content-Type'] = source.contentType
          ..['X-Upload-Content-Length'] = source.length.toString()
          ..['Content-Type'] = 'application/json; charset=utf-8';

      //Set the upload type to 'resumable'
      params = new Map.from(params);
      params['uploadType'] = 'resumable';

      var uploadRpc = new RpcRequest(
          "/b/$bucket/o",
          method: "POST",
          query: params,
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

        var initToken = new ResumeToken(
            Uri.parse(location),
            selector: params['fields']
        );
        return _sendUploadRequest(initToken, source);
      });
    });
  }

  /**
   * Check the status of a partially uploaded [Source]. The argument must be the resume token initially
   * returned by `uploadObject`.
   *
   * Returns a [ResumeToken] which can be used to resume the uploaded with the remainder of the source.
   */
  Future<ResumeToken> resumeUpload(ResumeToken resumeToken, Source source) {
    //Get the upload status
    var contentRange = new ContentRange(null, source.length);

    RpcRequest request = new RpcRequest(resumeToken.uploadUri,method: "PUT")
         ..headers['content-range'] = contentRange.toString();

    var completer = new Completer<RpcResponse>();

    return _client.send(request).then((response) {
      if (response.statusCode == HttpStatus.OK ||
          response.statusCode == HttpStatus.CREATED) {
        completer.complete(response);

      } else if (response.statusCode == HttpStatus.RESUME_INCOMPLETE) {
        var range = null;
        if (response.headers['range'] != null) {
          range = Range.parse(response.headers['range']);
        }
        var token = new ResumeToken.fromToken(
            resumeToken,
            range: range
        );
        return _sendUploadRequest(token, source);
      }
    });
  }

 ResumeToken _sendUploadRequest(
      ResumeToken token,
      Source source
  ) {
    var rangeToUpload;
    if (token.range != null) {
      rangeToUpload = new Range(token.range.hi + 1, source.length - 1);
    } else {
      rangeToUpload = new Range(0, source.length -1);
    }

    var contentRange = new ContentRange(rangeToUpload, source.length);
    var contentLength = source.length - rangeToUpload.lo;

    Completer<RpcResponse> completer = new Completer();
    var request = new StreamedRpcRequest(token.uploadUri, method: 'PUT')
        ..headers.putIfAbsent('content-type', () => source.contentType)
        ..headers['content-length'] = contentLength.toString();

    if (contentRange.range.isNotEmpty)
      request.headers['content-range'] = contentRange.toString();

    completeWithResponse(RpcResponse response) {
      if (completer.isCompleted) return;
      completer.complete(
          new StorageObject.fromJson(
              response.jsonBody,
              selector: token.selector)
      );
    }

    completeWithObject(StorageObject object) {
      if (completer.isCompleted) return;
      completer.complete(object);
    }

    completeWithError(err, [StackTrace stack]) {
      if (completer.isCompleted) return;
      completer.completeError(err, stack);
    }

    request
        .addSource(source, rangeToUpload.lo)
        .catchError(completeWithError);

    _client.send(request)
        .then((RpcResponse response) {
          if (response.statusCode == HttpStatus.OK
              || response.statusCode == HttpStatus.CREATED) {
            completeWithResponse(response);
          } else if (response.statusCode == HttpStatus.PARTIAL_CONTENT) {
            return resumeUpload(token, source).then((token) {
              return token.done.then(completeWithObject);
            }).catchError(completeWithError);
          } else {
            completeWithError(new RpcException.invalidStatus(response));
          }
        })
        .catchError((err, stackTrace) {
          if (!completer.isCompleted)
            completer.completeError(err, stackTrace);
        });

    return new ResumeToken.fromToken(
        token,
        range: token.range,
        completer: completer
    );
  }


  /**
   * Upload an object using the multipart/related upload API. This can be
   * slightly more efficient than using the resumable upload API for small
   * files (<5MB in size) but the upload must be retried in full on failure.
   *
   * This method automatically sets the `uploadType` parameter to `'multipart'`
   */
  Future<StorageObject> uploadObjectMultipart(
      String bucket,
      var /* String | StorageObject */ object,
      Source source,
      { Map<String, String> params: const {} }
  ) {
    return source.read(source.length).then((bytes) {
      if (object is String) {
        object = new StorageObject(bucket, object, selector: 'bucket,name');
      } else if (object is! StorageObject) {
        throw new ArgumentError('Expected a `String` or `StorageObject`');
      }

      //Set the upload type to 'resumable'
      params = new Map.from(params);
      params['uploadType'] = 'multipart';

      var uploadRpc = new MultipartRelatedRpcRequest(
          "/b/$bucket/o",
          method: "POST",
          query: params,
          isUploadRequest: true);

      uploadRpc.requestParts
              ..add(new RpcRequestPart(_JSON_CONTENT)..jsonBody = object)
              ..add(new RpcRequestPart(source.contentType)..bodyBytes = bytes);

      return _client.send(uploadRpc)
          .then(
              (response) {
                if (response.statusCode < 200 || response.statusCode >= 300)
                  throw new RpcException.invalidStatus(response);
                return new StorageObject.fromJson(response.jsonBody);
              });
    });
  }

  /**
   * Upload the object using the simple API media upload API.
   *
   * This method automatically sets the `uploadType` parameter to `media`
   */
  Future<StorageObject> uploadObjectSimple(
      String bucket,
      String object,
      Source source,
      { Map<String,String> params: const {} }) {
    return source.read(source.length).then((bytes) {

      params = new Map.from(params)
          ..['uploadType'] = 'media'
          ..['name'] = object;

      var headers = new Map<String,String>()
          ..['content-type'] = source.contentType;

      var uploadRpc = new RpcRequest(
          '/b/$bucket/o',
          method: 'POST',
          query: params,
          isUploadRequest: true,
          headers: headers
      );


      return _client.send(uploadRpc)
          .then(
              (response) {
                if (response.statusCode < 200 || response.statusCode >= 300)
                  throw new RpcException.invalidStatus(response);
                return new StorageObject.fromJson(response.jsonBody);
              });
    });
  }
}
