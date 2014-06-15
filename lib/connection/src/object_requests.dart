part of connection;

/**
 * Mixin class which contains rpc definitions which are specific
 * to fetching objects from a connection.
 */
abstract class ObjectRequests implements ConnectionBase {

  /**
   * Get the [:object:] from the given [:bucket:].
   *
   * [:selector:] specifies what fields should be included in a partial
   * response of the object and can be used to limit response sizes.
   *
   * [:generation:] fetches a specific version of the object, rather than the
   * latest version (default).
   *
   * [:ifGenerationMatch:] only returns the object if it's [:generation:]
   * matches the provided value. If both [:ifGenerationMatch:] and [:generation:]
   * are provided, the values must be identical for the method to return a
   * result.
   * [:ifGenerationNotMatch:] only returns the object if it's [:generation:]
   * does not match the provided value.
   * [:ifMetagenerationMatch:] only returns the object if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] only returns the object if its [:metageneration:]
   * does not match the provided value.
   *
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the [:bucket:].
   *
   * Returns a future which completes with the selected object.
   */
  Future<StorageObject> getObject(
      String bucket,
      String object,
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String selector: '*'
      }) {
    return new Future.sync(() {
      var query = new _Query(projectId)
          ..['generation'] = generation
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['projection'] = projection
          ..['fields'] = selector;

      object = _urlEncode(object);

      return _remoteProcedureCall(
          "/b/$bucket/o/$object",
          query: query
      );
    })
    .then((response) => new StorageObject.fromJson(response.jsonBody, selector: selector));
  }

  /**
   * Delete the [:object:] in the [:bucket:].
   *
   * If [:generation:] is provided, deletes that specific version of the object.
   *
   * [:ifGenerationMatch:] only deletes the object if it's [:generation:]
   * matches the provided value. If both [:ifGenerationMatch:] and [:generation:]
   * are provided, the values must be identical for the method to return a
   * result.
   * [:ifGenerationNotMatch:] only deletes the object if it's [:generation:]
   * does not match the provided value.
   * [:ifMetagenerationMatch:] only deletes the object if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] only deletes the object if its [:metageneration:]
   * does not match the provided value.
   */
  Future deleteObject(
      String bucket,
      String object,
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch
      }) {
    return new Future.sync(() {
      var query = new _Query(projectId)
          ..['generation'] = generation
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch;

      object = _urlEncode(object);
      return _remoteProcedureCall(
          "/b/$bucket/o/$object",
          method: "DELETE",
          query: query);
    });
  }

  /**
   * Update an [:object:], modifying only those fields specified by [:readSelector:]
   *
   * Implements a *read, modify, update* loop, which ensures that only fields which are
   * intentionally modified are updated on the server.
   *
   * First, the object is fetched with the [:readSelector:]. The fetched bucket is
   * passed into the `modify` function, which can only edit the selected fields.
   * The modified bucket is then patched onto the bucket metadata on the server.
   * A storage bucket, with fields populated from [:resultSelector:] is returned
   * as a partial response.
   *
   * If a [:resultSelector:] is not provided, defaults to the value provided for
   * [:readSelector:]
   *
   * [:generation:] selects a specific version of the object to update.
   * Default is the latest version.
   *
   * [:ifGenerationMatch:] only updates the object if it's [:generation:]
   * matches the provided value. If both [:ifGenerationMatch:] and [:generation:]
   * are provided, the values must be identical for the method to return a
   * result.
   * [:ifGenerationNotMatch:] only updates the object if it's [:generation:]
   * does not match the provided value.
   * [:ifMetagenerationMatch:] only updates the object if it's [:metageneration:]
   * matches the provided value
   * [:ifMetagenerationNotMatch:] only updates the object if its [:metageneration:]
   * does not match the provided value.
   *
   * [:destinatinPredefinedAcl:] is a [PredefinedAcl] to apply to the object.
   * Default is [PredefinedAcl.PRIVATE].
   */
  Future<StorageObject> updateObject(
      String bucket,
      String object,
      void modify(StorageObject object),
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        PredefinedAcl predefinedAcl: PredefinedAcl.PRIVATE,
        String projection: 'noAcl',
        String readSelector: '*',
        String resultSelector
      }) {
    return new Future.sync(() {
      var headers = new Map()
        ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      var query = new _Query(projectId)
          ..['generation'] = generation
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagnerationNotMatch'] = ifMetagenerationNotMatch
          ..['predefinedAcl'] = predefinedAcl
          ..['projection'] = projection
          ..['fields'] = readSelector;

      resultSelector = (resultSelector != null) ? resultSelector : readSelector;


      return _readModifyPatch(
          "/b/$bucket/o/${_urlEncode(object)}", query, headers, modify,
          readHandler: (rpcResponse) => new StorageObject.fromJson(rpcResponse.jsonBody, selector: readSelector),
          resultSelector: resultSelector
      ).then((response) => new StorageObject.fromJson(response.jsonBody, selector: resultSelector));
    });
  }

  /**
   * Copy the [:sourceObject:] in [:sourceBucket:]
   * to the [:destinationObject:] in the [:destinationBucket:]
   *
   * If [:destinationObject:] is a [String], metadata values will the same as those on
   * those in the [:sourceObject:]
   *
   * [:ifSourceGenerationMatch:] only copies the object if it's [:generation:]
   * matches the provided value. If both [:ifGenerationMatch:] and [:sourceGeneration:]
   * are provided, the values must be identical for the method to return a
   * result.
   * [:ifSourceGenerationNotMatch:] only copies the object if it's [:generation:]
   * does not match the provided value.
   * [:ifSourceMetagenerationMatch:] only copies the object if it's [:metageneration:]
   * matches the provided value
   * [:ifSourceMetagenerationNotMatch:] only copies the object if its [:metageneration:]
   * does not match the provided value.
   * [:ifDestinationGenerationMatch:] only overwrites the destination object if it's [:generation:]
   * matches the provided value.
   * [:ifDestinationGenerationNotMatch:] only overwrites the destination object if it's [:generation:]
   * does not match the provided value.
   * [:ifDestinationMetagenerationMatch:] only overwrites the destination object if it's [:metageneration:]
   * matches the provided value
   * [:ifDestinationMetagenerationNotMatch:] only overwrites the destination object if its [:metageneration:]
   * does not match the provided value.
   *
   * [:destinatinPredefinedAcl:] is a [PredefinedAcl] to apply to the object.
   * Default is [PredefinedAcl.PRIVATE].
   *
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the [:bucket:].
   *
   * The request returns the metadata of the [StorageObject] created at [:destinationObject:]
   * with fields selected by the given [:selector:].
   */
  Future<StorageObject> copyObject(
      String sourceBucket,
      String sourceObject,
      String destinationBucket,
      var /* String | StorageObject */ destinationObject,
      { int sourceGeneration,
        int ifSourceGenerationMatch,
        int ifSourceGenerationNotMatch,
        int ifSourceMetagenerationMatch,
        int ifSourceMetagenerationNotMatch,
        int ifDestinationGenerationMatch,
        int ifDestinationGenerationNotMatch,
        int ifDestinationMetagenerationMatch,
        int ifDestinationMetagenerationNotMatch,
        String projection: 'noAcl',
        PredefinedAcl destinationPredefinedAcl: PredefinedAcl.PROJECT_PRIVATE,
        String selector: '*'
      }) {
    return new Future.sync(() {

      if (destinationObject is String) {
        destinationObject = new StorageObject(destinationBucket, destinationObject, selector: selector);
      } else if (destinationObject is! StorageObject) {
        throw new ArgumentError("Expected a String or StorageObject");
      }

      Map<String,String> headers = new Map<String,String>()
          ..[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;

      var query = new _Query(projectId)
          ..['sourceGeneration'] = sourceGeneration
          ..['ifSourceGenerationMatch'] = ifSourceGenerationMatch
          ..['ifSourceGenerationNotMatch'] = ifSourceGenerationNotMatch
          ..['ifSourceMetagenerationMatch'] = ifSourceMetagenerationMatch
          ..['ifSourceMetagnenerationNotMatch'] = ifSourceMetagenerationNotMatch
          ..['ifGenerationMatch'] = ifDestinationGenerationMatch
          ..['ifGenerationNotMatch'] = ifDestinationGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifDestinationMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifDestinationMetagenerationNotMatch
          ..['destinationPredefinedAcl'] = destinationPredefinedAcl
          ..['projection'] = projection
          ..['fields'] = selector;

      sourceObject = _urlEncode(sourceObject);
      var destObject = _urlEncode(destinationObject.name);
      return _remoteProcedureCall(
          "/b/$sourceBucket/o/$sourceObject/copyTo/b/$destinationBucket/o/$destObject",
          method: "POST",
          headers: headers,
          query: query,
          body: destinationObject);

    }).then((response) => new StorageObject.fromJson(response.jsonBody, selector: selector));

  }

  /**
   * Concatenate a list of objects into [:destinationObject:].
   *
   * [:sourceObjects:] is the list of objects to concatenate into the destination.
   * Each item in the list must either be a [String], which contains the name of
   * the bucket, or a [CompositionSource], which provides additional controls for
   * how to specify the object to use during concatenation.
   *
   * [:ifGenerationMatch:] only overwrites the destination object if its [:generation:]
   * matches the provided value. If both [:ifGenerationMatch:] and [:generation:]
   * are provided, the values must be identical for the method to return a
   * result.
   * [:ifMetagenerationMatch:] only overwrites the destination object if its [:metageneration:]
   * matches the provided value.
   *
   * [:destinationPredefinedAcl:] is a [PredefinedAcl] to apply to the destination object.
   * Default is [PredefinedAcl.PROJECT_PRIVATE].
   *
   * Returns a [Future] which completes with the metadata of the destination object,
   * selected with the given [:selector:]
   */
  Future<StorageObject> composeObjects(
      String destinationBucket,
      var /* String | StorageObject */ destinationObject,
      List</* String | CompositionSource */ dynamic> sourceObjects,
      { destinationPredefinedAcl: PredefinedAcl.PROJECT_PRIVATE,
        ifGenerationMatch,
        ifMetagenerationMatch,
        String selector: '*'
      }) {
    return new Future.sync(() {
      if (destinationObject is String) {
        destinationObject = new StorageObject(destinationBucket, destinationObject, selector: selector);
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

      var query = new _Query(projectId)
          ..['destinationPredefinedAcl'] = PredefinedAcl.PRIVATE
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['fields'] = selector;

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
          query: query,
          body: body);
    }).then((response) => new StorageObject.fromJson(response.jsonBody, selector: selector));
  }



  /**
   * Return a directory like listing of the given [:bucket:].
   * [:prefix:] filters all object in the bucket whose name starts
   * with the specified prefix.
   * [:delimiter:] collapses all objects which match the filter and which
   * a substring which matches the delimiter into a single result (which is
   * emitted as a left value in the stream).
   *
   * To list all contents of the bucket without imposing a virtual folder structure,
   * both [:delimiter:] and [:prefix:] should be set to `null`.
   *
   * [:selector:] is a partial result selector that is applied to
   * right values emitted in the result stream.
   *
   * If [:maxResults:] is provided and non-negative, only the given number of
   * results is returned in result.
   * [:projection:] must be one of:
   * - `noAcl` No Access control details are included in the response (default)
   * - `full` Access control details are specified on the response. The user making
   * the request must have *OWNER* privileges for the bucket.
   *
   * If [:versions:] is `true`, then different versions of the same object
   * are returned as separate right values in the stream.
   *
   * Returns a stream of [Either] objects where each element is
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
   */
  Stream<Either<String,StorageObject>> listBucket(
      String bucket,
      { String prefix,
        String delimiter,
        int maxResults: -1,
        String projection: 'noAcl',
        bool versions: false,
        String selector: "*"
      }) {
    var fields = "nextPageToken,prefixes,items";
    if (selector != "*") {
      fields = fields + "($selector)";
    }
    var query = new _Query(projectId)
        ..['maxResults'] = (maxResults >= 0) ? maxResults : null
        ..['projection'] = projection
        ..['delimiter'] = delimiter
        ..['versions'] = versions
        ..['prefix'] = prefix
        ..['fields'] = fields;

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

    return _pagedRemoteProcedureCall("/b/$bucket/o", query: query)
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