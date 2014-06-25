part of connection;

/**
 * Mixin class for connection which implement RPC calls which modify
 * and return buckets.
 */
abstract class BucketRequests implements ConnectionBase {

  /**
   * Get the bucket with the specified [:name:], with selection [:selector:]
   *
   * [:queryParams:] is a map of optional query parameters to pass to the method. Infomation
   * about the valid entries in the map can be found in the [API documenation][0].
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/buckets/get
   */
  Future<StorageBucket> getBucket(String name, { Map<String,String> queryParams: const {} }) {
      return _remoteProcedureCall("/b/$name",query: queryParams)
          .then((rpcResponse) => new StorageBucket.fromJson(rpcResponse.jsonBody, selector: queryParams['fields']));
  }

  /**
   * List all buckets associated with the current project.
   *
   * [:queryParams:] is a map of optional query parameters to pass to the method. Infomation
   * about the valid entries in the map can be found in the [API documenation][0].
   *
   * NOTE:
   * - [:pageToken:] is an invalid query parameter to use in the method
   * - If provided the [:fields:] parameter should be a selector on the [bucket][1] resource, rather than
   * a selector on the returned page.
   *
   * Returns a [Stream] which emits results containing listed [StorageBucket]s
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/buckets/list
   * [1]: https://developers.google.com/storage/docs/json_api/v1/buckets
   */
  Stream<StorageBucket> listBuckets({ Map<String,String> queryParams: const {}}) {

    queryParams = new Map.from(queryParams);

    if (queryParams.containsKey('pageToken')) {
      throw new InvalidParameterException('\'pageToken\' is not a valid parameter for this method');
    }

    //The selector to use when reading the response
    var selector = '*';
    //The fiels parameter for the response.
    var fields = "nextPageToken,items";
    if (queryParams['fields'] != null) {
      var s = Selector.parse(queryParams['fields']);
      if (s.isPathInSelection(new FieldPath('items'))) {
        throw new InvalidParameterException(
            "'fields' must be a selector on the bucket resource, "
            "not the page response");
      }

      selector = queryParams['fields'];
      fields += "(${queryParams['fields']})";
    }
    queryParams['fields'] = fields;

    logger.info("listing buckets in project");
    return _pagedRemoteProcedureCall("/b", query:queryParams)
        .expand((page) =>
            page['items']
            .map((item) => new StorageBucket.fromJson(item, selector: selector))
        );
  }

  /**
   * Create an empty bucket in storage.
   *
   * [:bucket:] can be either a [String] or [StorageBucket]. If a [String], the
   * bucket will be created with server default values. Otherwise, properties
   * will be overriden with the values from the provided bucket.
   *
   * [:queryParams:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * NOTE:
   * - If `bucket` is a [StorageBucket], the bucket [:name:] must be selected and the name
   * must be a valid according to the [google bucket naming specifications][1]
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/buckets/insert
   * [1]: https://developers.google.com/storage/docs/bucketnaming
   */
  Future<StorageBucket> createBucket(
      /* String | StorageBucket */ bucket,
      { Map<String,String> queryParams: const {} }) {
    return new Future.sync(() {
      if (bucket is String) {
        bucket = new StorageBucket(bucket, selector: 'name');
      } else if (bucket is StorageBucket) {
        if (!bucket.hasField('name')) {
          throw new ArgumentError("'name' must be selected when creating resource");
        }
      }

      _checkValidBucketName(bucket.name);

      var headers = new Map<String,String>()
            ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      logger.info("Creating bucket ${bucket.name}");
      return _remoteProcedureCall(
          "/b",
          method: "POST",
          query: queryParams,
          headers: headers,
          body: bucket);
    }).then((response) => new StorageBucket.fromJson(response.jsonBody, selector: queryParams['fields']));
  }

  /**
   * Deletes an empty storage bucket. Returns a future which completes with `null`
   * when the delete is done
   *
   * [:queryParams:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * Returns a [Future] which completes with `null` on success.
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/buckets/delete
   */
  Future deleteBucket(String bucket, { Map<String,dynamic> queryParams }) {
    return new Future.sync(() {
      logger.info("deleting bucket ${bucket}");
      return _remoteProcedureCall("/b/$bucket", method: "DELETE", query: queryParams)
          .then((_) => null);
    });
  }

  /**
   * Update a bucket on the server, using `HTTP PUT` semantics.
   *
   *  [:queryParams:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * NOTES:
   * - If the [:fields:] parameter is provided and the bucket selector is not *any* (`'*'`), the
   * two values must be identical (or an exception will be raised). Otherwise, the [:fields:]
   * parameter will be set to the most specific of the provided selectors.
   * - Due to `HTTP PUT` semantics, if using a partial request and any required resource field
   * is not provided, the server will respond with an error. Also, if a previously set value is not
   * included in the partial bucket metadata, the value on the server will be overwritten with `null`.
   *
   * For these reason, the `patchBucket` method is recommended as a safer alternative.
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/buckets/update
   */
  Future<StorageBucket> updateBucket(
      StorageBucket bucket,
      { Map<String,String> queryParams: const {} }) {
    return new Future.sync(() {
      queryParams = new Map.from(queryParams);

      if (bucket.selector != '*') {
        if (queryParams['fields'] != null && queryParams['fields'] != bucket.selector) {
          throw new InvalidParameterException('Incompatible selectors');
        }
      }

      if (queryParams['fields'] != null) {
        bucket = new StorageBucket.fromJson(bucket.toJson(), selector: queryParams['fields']);
      } else {
        queryParams['fields'] = bucket.selector;
      }

      var headers = new Map<String,String>()
          ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      return _remoteProcedureCall(
          "/b/$bucket",
          method: "PUT",
          headers: headers,
          body: bucket
      )
      .then((response) => new StorageBucket.fromJson(response.jsonBody, selector: queryParams['fields']));

    });
  }

  /**
   * Update an [:object:] with safe partial request semantics.
   *
   * [:queryParams:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * If a [:fields:] parameter is provided, it is used to specify a partial
   * response. The [StorageBucket] returned by this response is passed into the
   * [:modify:] method and subsequently updated on the server.
   *
   * Only those fields selected by the [:fields:] parameter will be updated on the
   * server. Attempting to change a value which is not selected will raise an
   * `NotInSelectionError` when attempting to modify the value.
   *
   * To clear a field on the server resource, the value of the field must be
   * explicitly set to `null`.
   *
   * eg. To update the [:websiteConfiguration:] of the bucket without modifying
   * any other values,
   *
   *       connection.patchBucket('example-bucket', {'fields': 'websiteConfiguration' },
   *           (bucket) {
   *             bucket.website.mainPageSuffix = 'index.html';
   *             //Setting the value to `null` will clear the value of the field
   *             bucket.website.notFoundPage = null;
   *           });
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/buckets/patch
   * [1]: http://tools.ietf.org/html/rfc5789
   */
  Future<StorageBucket> patchBucket(
      String bucket,
      void modify(StorageBucket bucket),
      { Map<String,String> queryParams: const {} }) {
    return new Future.sync(() {
      var headers = new Map()
          ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      logger.info("Patching bucket ${bucket}");

      return _readModifyPatch(
          "/b/$bucket", queryParams, headers, modify,
          readHandler: (rpcResponse) => new StorageBucket.fromJson(rpcResponse.jsonBody, selector: queryParams['fields'])
      )
      .then((rpcResponse) => new StorageBucket.fromJson(rpcResponse.jsonBody, selector: queryParams['fields']));
    });
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