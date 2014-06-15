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
    var rpc = new RPCRequest(url, "GET",
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
          ..['X-Upload-Content-Type'] = mimeType
          ..['X-Upload-Content-Length'] = source.length.toString()
          ..['X-Upload-Content-MD5'] = CryptoUtils.bytesToBase64(contentMd5);

      var query = new _Query(projectId)
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['predefinedAcl'] = predefinedAcl
          ..['projection'] = projection
          ..['fields'] = selector;

     // If the request fails between the last byte of data sent and returning the object metadata, we
     // need to have a way of retrieving the metadata.
     var getObjectRpc = new RpcRequest("/b/$bucket/o/${object.name}", query: query);

      //Set the upload type to 'resumable'
      query['uploadType'] = 'resumable';

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

        return new ResumeToken(
            ResumeToken.TOKEN_INIT,
            Uri.parse(location),
            //FIXME (ovangle): Setting `-1` for the high range makes [Range] unparseable.
            new Range(0, -1),
            getObjectRpc
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
          var uploadedRange = new Range(0, source.length - 1);
          return new ResumeToken.fromToken(resumeToken, ResumeToken.TOKEN_COMPLETE, uploadedRange);
        }

        if (response.statusCode == HttpStatus.PARTIAL_CONTENT ||
            response.statusCode == 308 /* Resume Incomplete */) {
          var range = response.headers['range'];
          if (range == null) throw new RpcException.expectedResponseHeader('range', response);
          return new ResumeToken.fromToken(resumeToken, ResumeToken.TOKEN_INTERRUPTED, Range.parse(range));
        }

        throw new RpcException.invalidStatus(response);
      });
    });
  }

  Future<StorageObject> resumeUpload(ResumeToken resumeToken, Source source) {
    return new Future.sync(() {
      if (resumeToken.isComplete)
        throw new StateError('Upload already complete');
      print('Resuming upload');

      var uploadId = resumeToken.uploadUri.queryParameters['upload_id'];
      logger.info("Resuming upload $uploadId");
      logger.info("At byte: ${resumeToken.range.hi + 1}");
      logger.info("Bytes remaining: ${source.length - resumeToken.range.hi}");

      var uploadRange = new ContentRange(
          new Range(resumeToken.range.hi + 1, source.length - 1),
          source.length
      );

      var request = new StreamedRpcRequest(resumeToken.uploadUri, method: "PUT")
          ..headers['content-range'] = uploadRange.toString();


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

      return _client.send(request, retryRequest: false).then((response) {


        var selector = resumeToken.getObjectRequest.query['fields'];
        handler(RpcResponse response) =>
            new StorageObject.fromJson(response.jsonBody, selector: selector);

        if (_RETRY_STATUS.contains(response.statusCode)) {
          return getUploadStatus(resumeToken, source).then((resumeToken) {
            if (resumeToken.isComplete) {
              //Send the (stored) request to get the object metadata.
              return _client.send(resumeToken.getObjectRequest)
                  .then(handler);
            } else {
              //Othwerise we still have bytes to upload. Resume the upload.
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
