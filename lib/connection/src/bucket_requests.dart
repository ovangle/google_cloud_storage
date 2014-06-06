part of connection;

/**
 * Mixin class for connection which implement RPC calls which modify
 * and return buckets.
 */
abstract class BucketRequests implements ConnectionBase {

  /**
   * Get the bucket with the specified [:name:], with selection [:selector:]
   *
   * [:ifMetagenerationMatch:] only returns the bucket if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] only returns the bucket if its [:metageneration:]
   * does not match the provided value.
   *
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the project.
   *
   * Returns a future which completes with the selected bucket.
   */
  Future<StorageBucket> getBucket(
      String name,
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String selector: "*"
      }) {
    return new Future.sync(() {
      var query = new _Query(projectId)
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['projection'] = projection
          ..['fields'] = selector;
      logger.info("Fetching bucket $name");
      return _remoteProcedureCall(
          "/b/$name",
          query: query,
          handler: _handleStorageBucketResponse(selector)
      );
    });
  }

  /**
   * List all buckets associated with the current bucket.
   *
   * If [:maxResults:] is provided and non-negative, only the given number of
   * results is returned in result.
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the project.
   *
   * Returns a [Stream] which emits results containing selected buckets
   */
  Stream<StorageBucket> listBuckets(
      { int maxResults: -1,
        String projection: 'noAcl',
        String selector: '*'
      }) {
    var fields = "nextPageToken,items";
    if (selector != "*") {
      fields += "($selector)";
    }
    var query = new _Query(projectId)
        ..['maxResults'] = (maxResults >= 0) ? maxResults : null
        ..['projection'] = projection
        ..['fields'] = fields;

    logger.info("listing buckets in project");
    return _pagedRemoteProcedureCall("/b", query:query)
        .expand((page) =>
            page['items']
            .map((item) => new StorageBucket.fromJson(item, selector: selector))
        );
  }

  /**
   * Create an empty storage bucket.
   * [:bucket:] can be either a [String] or [StorageBucket]. If a [String], the
   * bucket will be created with server default values. Otherwise, properties
   * will be overriden with the values from the provided bucket.
   *
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the project.
   *
   * The selector must select the path containing the bucket [:name:] and the name
   * must be a valid bucket name according to the [google bucket naming specifications][0]
   *
   *
   * [0]: https://developers.google.com/storage/docs/bucketnaming
   */
  Future<StorageBucket> createBucket(
      /* String | StorageBucket */ bucket,
      { String projection: 'noAcl',
        String selector: '*'
      }) {
    return new Future.sync(() {
      //Check that the bucket name is in the selector.
      var s = Selector.parse(selector);
      if (!s.isPathInSelection(new FieldPath("name")))
        throw new ArgumentError("'name' must be selected");

      if (bucket is String) {
        bucket = new StorageBucket(bucket, selector: selector);
      } else if (bucket is! StorageBucket) {
        throw new ArgumentError("Expected a String or StorageBucket");
      }

      _checkValidBucketName(bucket.name);

      var query = new _Query(projectId)
          ..['projection'] = projection
          ..['fields'] = selector;

      var headers = new Map<String,String>()
            ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      logger.info("Creating bucket ${bucket.name}");
      return _remoteProcedureCall(
          "/b",
          method: "POST",
          query: query,
          headers: headers,
          body: bucket,
          handler: _handleStorageBucketResponse(selector));
    });
  }

  /**
   * Deletes an empty storage bucket. Returns a future which completes with `null`
   * when the delete is done
   *
   * [:ifMetagenerationMatch:] only deletes the bucket if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] only deletes the bucket if its [:metageneration:]
   * does not match the provided value.
   */
  Future deleteBucket(
      String bucket,
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch
      }) {
    return new Future.sync(() {
      var query = new _Query(projectId)
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch;

      logger.info("deleting bucket ${bucket}");
      return _remoteProcedureCall(
          "/b/$bucket",
          method: "DELETE",
          query: query,
          handler: _handleEmptyResponse)
          .whenComplete(() => "bucket $bucket deleted");
    });
  }

  /**
   * Update a bucket, modifying only those fields which are selected by [:readSelector:]
   *
   * Implements a *read, modify, update* loop, which ensures that only fields which are
   * intentionally modified are updated on the server.
   *
   * First, the bucket is fetched with the [:readSelector:]. The fetched bucket is
   * passed into the `modify` function, which can only edit the selected fields.
   * The modified bucket is then patched onto the bucket metadata on the server.
   * A storage bucket, with fields populated from [:resultSelector:] is returned
   * as a partial response.
   *
   * If a [:resultSelector:] is not provided, defaults to the value provided for
   * [:readSelector:]
   *
   * [:ifMetagenerationMatch:] only patches the bucket if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] only patches the bucket if its [:metageneration:]
   * does not match the provided value.
   *
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the project.
   *
   * eg. To update the [:websiteConfiguration:] of the bucket without modifying
   * any other values,
   *
   *       connection.patchBucket('example', 'websiteConfiguration',
   *           (bucket) {
   *             bucket.website.mainPageSuffix = 'index.html';
   *             //Setting the value to `null` will clear the value of the field
   *             bucket.website.notFoundPage = null;
   *           },
   *           resultSelector: 'name,websiteConfiguration');
   */
  Future<StorageBucket> updateBucket(
      String bucket,
      void modify(StorageBucket bucket),
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection,
        String readSelector: "*",
        String resultSelector
      }) {
    return new Future.sync(() {
      var query = new _Query(projectId)
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['projection'] = projection
          ..['fields'] = readSelector;

      var headers = new Map();
      headers[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      resultSelector = (resultSelector != null) ? resultSelector: readSelector;

      logger.info("Patching bucket ${bucket}");

      return _readModifyPatch(
          "/b/$bucket", query, headers, modify,
          readHandler: _handleStorageBucketResponse(readSelector),
          resultSelector: resultSelector,
          resultHandler: _handleStorageBucketResponse(resultSelector)
      ).whenComplete(() => "patched bucket ${bucket} successfully");
    });
  }

  /**
   * A response handler that handles responses which are expected to contain
   * a single [StorageBucket] (with fields selected by the given [:selector:])
   */
  _ResponseHandler _handleStorageBucketResponse(String selector) {
    return (_RemoteProcedureCall rpc, http.BaseResponse response) =>
        _handleJsonResponse(rpc, response)
        .then((json) => new StorageBucket.fromJson(json, selector: selector));
  }

}

final _BUCKET_NAME = new RegExp(r'^[a-z0-9]([a-zA-Z0-9_.-]+)[a-z0-9]$');
final _IP = new RegExp(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$');


/**
 * Checks the bucket name is valid, according to [google specifications][0]
 *
 * [0]: https://developers.google.com/storage/docs/bucketnaming
 */
void _checkValidBucketName(String name) {
  if (name.indexOf(_BUCKET_NAME) < 0)
    throw new ArgumentError("Bucket name must match ${_BUCKET_NAME.pattern}");
  if (!name.contains('.') && name.length > 63)
    throw new ArgumentError("Bucket names not containing '.' are limited to 63 characters");
  if (name.contains('.')) {
    if (name.length > 222) {
      throw new ArgumentError("Bucket names containing '.' are limited to 222 characters");
    }
    var comps = name.split('.');
    if (comps.any((comp) => comp.length > 63)) {
      throw new ArgumentError("Each dot-separated component of a bucket name is limited to 63 characters");
    }
  }
  if (name.indexOf(_IP) >= 0)
    throw new ArgumentError("Bucket name cannot be an IP address in dot-separated notation");
  if (name.startsWith('goog'))
    throw new ArgumentError("Bucket name cannot start with 'goog' prefix");
}