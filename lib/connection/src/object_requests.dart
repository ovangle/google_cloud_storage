part of connection;

/**
 * Mixin class which contains rpc definitions which are specific
 * to fetching objects from a connection.
 */
abstract class ObjectRequests implements ConnectionBase {

  /**
   * Get the [:object:] from the given [:bucket:].
   *
   * [:params:] is a map of optional query parameters to pass to the method. Infomation
   * about the valid entries in the map can be found in the [API documenation][0].
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/objects/get
   */
  Future<StorageObject> getObject(String bucket, String object,
      { Map<String,String> params: const {} }) {
    return _remoteProcedureCall("/b/$bucket/o/${_urlEncode(object)}", query: params)
        .then((response) => new StorageObject.fromJson(response.jsonBody, selector: params['fields']));
  }

  /**
   * Removes the given [:object:] from the [:bucket:].
   *
   *  [:params:] is a map of optional query parameters to pass to the method. Infomation
   * about the valid entries in the map can be found in the [API documenation][0].
   *
   * Returns a [Future] which completes with `null` on success.
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/objects/delete
   */
  Future deleteObject(String bucket, String object,
      { Map<String,dynamic> params }) {
    return new Future.sync(() {
      logger.info("Deleting '$object' from '$bucket'");
      object = _urlEncode(object);
      return _remoteProcedureCall("/b/$bucket/o/$object", method: "DELETE", query: params)
          .then((_) => null);
    });
  }

  /**
   * Update an [:object:] with safe partial request semantics.
   *
   * [:params:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * If a [:fields:] parameter is provided, it is used to specify a partial
   * response. The [StorageObject] returned by this response is passed into the
   * [:modify:] method and subsequently updated on the server.
   *
   * Only those fields selected by the [:fields:] parameter will be updated on the
   * server. Attempting to change a value which is not selected will raise an
   * `NotInSelectionError` when attempting to modify the value.
   *
   * To clear a field on the server resource, the value of the field must be
   * explicitly set to `null`.
   *
   * eg. The following will update the `contentType` and clear the `contentLanguage` without
   * fetching or modifying any other fields on the resource.
   *
   *      connection.patchObject(
   *          'example-bucket', 'example-object',
   *          (object) {
   *            object.contentType = 'text/plain';
   *            object.contentLanguage = null;
   *          }
   *          params: { 'fields': 'contentType,contentLanguage' });
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/objects/patch
   */
  Future<StorageObject> patchObject(
      String bucket,
      String object,
      void modify(StorageObject object),
      { Map<String,String> params: const {} }) {
    return new Future.sync(() {
      var headers = new Map()
        ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      return _readModifyPatch(
          "/b/$bucket/o/${_urlEncode(object)}", params, headers, modify,
          readHandler: (rpcResponse) => new StorageObject.fromJson(rpcResponse.jsonBody, selector: params['fields'])
      ).then((response) => new StorageObject.fromJson(response.jsonBody, selector: params['fields']));
    });
  }

  /**
   * Copy the [:sourceObject:] in [:sourceBucket:] to the [:destinationObject:] in the [:destinationBucket:]
   *
   * [:params:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/objects/copy
   */
  Future<StorageObject> copyObject(
      String sourceBucket,
      String sourceObject,
      String destinationBucket,
      String destinationObject,
      { Map<String,String> params: const {} }) {
        return new Future.sync(() {

          Map<String,String> headers = new Map<String,String>()
              ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;


          sourceObject = _urlEncode(sourceObject);
          var destObject = _urlEncode(destinationObject);

          return _remoteProcedureCall(
              "/b/$sourceBucket/o/$sourceObject/copyTo/b/$destinationBucket/o/$destObject",
              method: "POST",
              headers: headers,
              query: params
          );

        }).then((response) => new StorageObject.fromJson(response.jsonBody, selector: params['fields']));
  }

  /**
   * Concatenate a list of objects into [:destinationObject:].
   *
   * [:sourceObjects:] is the list of objects to concatenate into the destination.
   * Each item in the list must either be a [String], which contains the name of
   * the bucket, or a [CompositionSource], which provides additional controls for
   * how to specify the object to use during concatenation.
   *
   * [:params:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/objects/copy
   */
  Future<StorageObject> composeObjects(
      String destinationBucket,
      var /* String | StorageObject */ destinationObject,
      List</* String | CompositionSource */ dynamic> sourceObjects,
      { Map<String,String> params: const {}}) {
    return new Future.sync(() {
      if (destinationObject is String) {
        destinationObject = new StorageObject(destinationBucket, destinationObject, selector: "bucket,name");
      } else if (destinationObject is! StorageObject) {
        throw new ArgumentError("Expected a String or StorageObject");
      }
      sourceObjects = sourceObjects
          .map((obj) {
            if (obj is String) {
              return new CompositionSource(obj);
            } else if (obj is CompositionSource) {
              return obj;
            }
            throw new ArgumentError("All items in sourceObjects expected to be either String or CompositionSource");
          })
          .toList(growable: false);

      var headers = new Map()
          ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      var body =
        { 'kind': 'storage#composeRequest',
          'sourceObjects': sourceObjects,
          'destination': destinationObject
        };

      var destObject = _urlEncode(destinationObject.name);
      return _remoteProcedureCall(
          "/b/$destinationBucket/o/$destObject",
          method: "POST",
          headers: headers,
          query: params,
          body: body);
    }).then((response) => new StorageObject.fromJson(response.jsonBody, selector: params['fields']));
  }



  /**
   * Return a directory like listing of the given [:bucket:].
   *
   * [:params:] is a map of optional query parameters to pass to the method. Information
   * about valid entries in the map can be found in the [API documentation][0].
   *
   * NOTES:
   * - [:pageToken:] is an invalid query parameter for this method
   * - If provided, the [:fields:] selector must be specified as a selector on the
   * [object][1] resource.
   *
   * If a delimiter is provided in the returned map, the result will be a
   * [Stream] of [Either] objects, where each element is:
   * - A right value if the object's name matches `'^$prefix([$^delimiter])*\$'
   * - A left value if the object's name matches `'^$prefix([^delimiter])+.*$'`
   *  In this case, the inner value of the match will contain the object's name
   *  up to the next delimiter after the prefix.
   *
   * eg. If `'/'` is the provided delimiter,
   *     bucket
   *      |-- folder1/
   *      |    |-- folder1/subfolder1
   *      |    |    |-- folder1/subfolder1/file1
   *      |    |    `-- folder1/subfolder1/file2
   *      |    |-- folder1/file1
   *      |    |-- folder1/file2
   *      |    `-- folder1/file3
   *      |-- folder2/
   *      `-- file1
   *
   * listing the folder with with prefix `''` would a stream with elements
   *     [ Left 'folder1/',
   *       Left 'folder2/',
   *       Right StorageObject ('file1')
   *     ]
   *
   * listing the folder contents with the prefix `'folder1/'` would return
   * a stream with elements
   *     [ Left 'folder1/subfolder1/',
   *       Right StorageObject (folder1/file1),
   *       Right StorageObject (folder1/file1),
   *       Right StorageObject (folder1/file2),
   *       Right StorageObject (folder1/file3)
   *     ]
   *
   * Otherwise, the [Stream] will consist entirely of right values and should
   * be mapped to a [Stream<StorageObject>].
   *
   * [0]: https://developers.google.com/storage/docs/json_api/v1/objects/list
   * [0]: https://developers.google.com/storage/docs/json_api/v1/objects
   */
  Stream<Either<String,StorageObject>> listObjects(
      String bucket,
      { Map<String, String > params: const {} }) {
    params = new Map.from(params);
    if (params.containsKey('pageToken')) {
      throw new InvalidParameterException("'pageToken' is not a valid parameter for this method");
    }

    //The selector to use when reading the objects from the result
    var selector = '*';
    //The actual field selector
    var fields = 'nextPageToken,prefixes,items';
    if (params['fields'] != null) {
      var s = Selector.parse(params['fields']);
      if (s.isPathInSelection(new FieldPath('items')) ||
          s.isPathInSelection(new FieldPath('prefixes'))) {
        throw new InvalidParameterException(
            "'fields' parameter must be a selector on the object resource, "
            "not the page token"
        );
      }
      selector = params['fields'];
      fields += '(${params['fields']})';
    }

    params['fields'] = fields;

    //Separate the current page of results into the prefixes of the folders up to
    //the next delimiter (after prefix) and the resources in the current folder
    Iterable<Either<String,StorageObject>> expandPage(Map<String,dynamic> result) {
      var results = new List<Either<String,StorageObject>>();

      var prefixes = result['prefixes'];
      if (prefixes != null) {
        results.addAll(
            prefixes.map((prefix) => new Either.ofLeft(prefix))
        );
      }

      var items = result['items'];
      if (items != null) {
        results.addAll(
            items.map((item) => new Either.ofRight(
                new StorageObject.fromJson(item, selector: selector)
            ))
        );
      }

      return results;
    }

    return _pagedRemoteProcedureCall("/b/$bucket/o", query: params)
        .expand(expandPage);
  }

  //TODO: Object change notifications.
}

class CompositionSource {
  /**
   * The name of a storage object to compose into the destination.
   * The object must exist in the same bucket as the destination.
   */
  final String name;
  /**
   * Select a specific generation of the object to use during the composition.
   * By default, the latest version of the object is used.
   */
  final int generation;
  /**
   * Only perform the composition if the [:generation:] of the
   * source object matches the value.
   * If [:ifGenerationMatch:] and [:generation:] are both provided, then
   * the values must match or the operation will fail.
   */
  final int ifGenerationMatch;

  CompositionSource(this.name, {this.generation, this.ifGenerationMatch});

  Map<String,dynamic> toJson() {
    var json = {'name': name};
    if (generation != null)
      json['generation'] = generation;
    if (ifGenerationMatch != null)
      json['objectPreconditions'] = {'ifGenerationMatch': ifGenerationMatch};
    return json;
  }
}

/**
 * An enum consisting of various predefined acl values which can be
 * set at object creation.
 */
class PredefinedAcl {

  /**
   * Object owner gets `OWNER` access
   */
  static const PRIVATE = const PredefinedAcl._('private');

  /**
   * Object owner gets `OWNER` access and `allAuthenticatedUsers`
   * get `READER` access
   */
  static const AUTHENTICATED_READ = const PredefinedAcl._('authenticatedRead');

  /**
   * Object owner gets `OWNER` access and `allUsers` get
   * `READER` access
   */
  static const PUBLIC_READ = const PredefinedAcl._('publicRead');

  /**
   * Object owner gets `OWNER` access and all project team owners
   * get `OWNER` access
   */
  static const BUCKET_OWNER_FULL_CONTROL = const PredefinedAcl._('bucketOwnerFullControl');

  /**
   * Object owner gets `OWNER` access and project team owners get
   * `READ` access
   */
  static const BUCKET_OWNER_READ = const PredefinedAcl._('bucketOwnerRead');


  /**
   * Object owner gets `OWNER` access and project team members
   * get access according to their roles.
   */
  static const PROJECT_PRIVATE = const PredefinedAcl._('projectPrivate');

  final String _value;

  static const List<String> values =
      const [ PRIVATE, AUTHENTICATED_READ, PUBLIC_READ,
              BUCKET_OWNER_FULL_CONTROL, BUCKET_OWNER_READ, PROJECT_PRIVATE
            ];

  const PredefinedAcl._(this._value);

  factory PredefinedAcl(String value) {
    var v = values.firstWhere((v) => v.toString() == value, orElse: () => null);
    if (v != null)
      return v;
    throw new ArgumentError("Invalid predefined acl value: '$v'");
  }

  String toString() => _value;
}