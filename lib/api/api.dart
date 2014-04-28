library google_cloud_storage.api;

import 'dart:async';
import 'dart:convert' show JSON, UTF8;
import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:collection/wrappers.dart';

import 'package:google_oauth2_client/google_oauth2_console.dart' as oauth2;

import '../either/either.dart';
import '../json/object.dart';

part 'src/access_controls.dart';
part 'src/entry.dart';
part 'src/misc.dart';


const API_VERSION = 'v1beta2';

const API_ENDPOINT = 'https://www.googleapis.com';

const API_SCOPES =
    const { PermissionRole.READER : 'https://www.googleapis.com/auth/devstorage.read_only',
            PermissionRole.WRITER : 'https://www.googleapis.com/auth/devstorage.read_write',
            PermissionRole.OWNER: 'https://www.googleapis.com/auth/devstorage.full_control'
          };

const _JSON_CONTENT = 'application/json; charset=UTF-8';
const _MULTIPART_CONTENT = 'multipart/related; boundary=content_boundary';


//'\r\n'
const _NEWLINE = const [0x0D, 0x0A];
//': '
const _COLON_SPACE = const [0x3A, 0x20];

const _CONTENT_BOUNDARY = 'content_boundary';
//--content_boundary
const _MULTIPART_CONTENT_SEPARATOR =
    const [ 0x2D, 0x2D, 0x63, 0x6F, 0x6E, 0x74, 0x65, 0x6E, 0x74, 0x5F,
            0x62, 0x6F, 0x75, 0x6E, 0x64, 0x61, 0x72, 0x79
          ];
//--content_boundary--
const _MULTIPART_CONTENT_TERMINATOR =
    const [ 0x2D, 0x2D, 0x63, 0x6F, 0x6E, 0x74, 0x65, 0x6E, 0x74, 0x5F,
            0x62, 0x6F, 0x75, 0x6E, 0x64, 0x61, 0x72, 0x79, 0x2D, 0x2D
          ];


Future _readPrivateKey(String path) {
  if (path == null)
    return new Future.value();
  return new File(path).readAsString();
}

class Connection {
  static Future<Connection> open(String projectNumber, String projectId, PermissionRole role,
      {String serviceAccount, String pathToPrivateKey}) {
    return _readPrivateKey(pathToPrivateKey).then((privateKey) {
      var scopes;
      if (serviceAccount != null && pathToPrivateKey != null) {
        scopes = [ 'https://www.googleapis.com/auth/userinfo.email',
                   API_SCOPES[role]].join(" ");
      }
      oauth2.ComputeOAuth2Console console = new oauth2.ComputeOAuth2Console(
          projectNumber,
          iss: serviceAccount,
          privateKey: privateKey,
          scopes: scopes);
      _sendAuthorisedRequest(http.Request request) =>
          console.withClient(
              (client) => client.send(request));
      return new Connection._(projectId, role, _sendAuthorisedRequest);
    });
  }

  final String projectId;
  /**
   * The role of the user who established this connection.
   */
  final PermissionRole role;
  final _sendAuthorisedRequest;

  Connection._(this.projectId, this.role, Future<http.StreamedResponse> this._sendAuthorisedRequest(http.Request request));

  /**
   * Get the platform url to submit a request.
   * - [:path:] is the path to the resource (eg. /b/<bucket>
   * - [:query:] is the parameters to pass to the url.
   * - [:apiBaseUrl:] The base url of the api endpoint
   * - [:apiVersion:] The version of the API to call.
   *
   * Returns the API endpoint url.
   */
  Uri platformUrl(String path,{ Map<String,String> query }) =>
      Uri.parse("$API_ENDPOINT/storage/${API_VERSION}${path}?${query}");

  Uri platformUploadUrl(String path, {Map<String,String> query}) =>
      Uri.parse("$API_ENDPOINT/upload/storage/${API_VERSION}${path}?${query}");

  /**
   * Submits a `JSON` RPC call to the cloud storage service.
   */
  Future<Map<String,dynamic>> _sendJsonRpc(
      String path,
      { String method: "GET",
        _Query query,
        Map<String,dynamic> body
      }) {
    assert(query != null);
    print(query);

    var url = platformUrl(path, query: query);
    http.Request request = new http.Request(method, url);

    if (!["GET", "DELETE"].contains(method)) {
      request.headers[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;
      request.bodyBytes = UTF8.encode(JSON.encode(body));
    }
    return _sendAuthorisedRequest(request)
        .then(http.Response.fromStream)
        .then(_handleJsonResponse);
  }

  Map<String,dynamic> _handleJsonResponse(http.Response response) {
    var method = response.request.method;
    var url = response.request.url;
    if (response.statusCode < 200 || response.statusCode >= 300)
      throw new RPCException.invalidStatus(response, method, url);
    if (response.statusCode == HttpStatus.NO_CONTENT) {
      assert(method == "DELETE");
      return null;
    }
    var contentType = ContentType.parse(response.headers[HttpHeaders.CONTENT_TYPE]);

    if (contentType.primaryType != 'application' || contentType.subType != 'json')
      throw new RPCException.expectedJSON(response, method, url);
    return JSON.decode(response.body);
  }

  /**
   *
   */
  Stream<Either<String,Map<String,dynamic>>> _streamJsonRpc(
      String path,
      { String method: "GET",
        Map<String,dynamic> query,
        Map<String,dynamic> body
      }) {
    StreamController<Either<String,Map<String,dynamic>>> controller = new StreamController();
    assert(query['fields'] != null);
    if (query['fields'] != "*") {
      query['fields'] = 'nextPageToken,prefixes,items(${query['fields']})';
    } else {
      query['fields'] = '*';
    }

    // Add the next page of the results (starting at `nextPageToken`)
    // to the controller.
    Future addNextPage(String nextPageToken) {

      var pageQuery = new _Query.from(query)
          ..['pageToken'] = nextPageToken;

      return _sendJsonRpc(path,
          method: method,
          query: pageQuery,
          body: body)
      .then((response) {
        var items = (response['items'] != null) ? response['items'] : [];
        for (var item in items) {
          if (!controller.isClosed)
            controller.add(new Either.ofRight(item));
        }
        var prefixes = (response['prefixes'] != null) ? response['prefixes'] : [];
        for (var prefix in prefixes) {
          if (!controller.isClosed) controller.add(new Either.ofLeft(prefix));
        }
        var nextPageToken = response['nextPageToken'];
        if (nextPageToken == null) {
          return controller.close();
        } else {
          return addNextPage(response['nextPageToken']);
        }
      })
      .catchError((err, stackTrace) => controller.addError(err, stackTrace));
    }
    addNextPage(null);

    return controller.stream;
  }


  /**
   * Get the bucket with the given name from the datastore.
   */
  Future<StorageBucket> getBucket(
      String bucket,
      { String selector: "*",
        String projection: 'noAcl',
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch}) {
    return new Future.sync(() =>
        new _Query(projectId)
            ..['ifMetagenerationMatch'] = ifMetagenerationMatch
            ..['ifMetagenerationNotMatch'] =ifMetagenerationNotMatch
            ..['projection'] = projection
            ..['fields'] = selector
      )
      .then((query) => _sendJsonRpc("/b/$bucket", method: "GET", query:query))
      .then((response) => new StorageBucket._(response, selector: selector));
  }

  Stream<StorageBucket> listBuckets(
      {int maxResults: -1,
       String projection: 'noAcl',
       String selector: "*"}) {
    var query = new _Query(projectId);
    try {
      query['maxResults'] = (maxResults >= 0) ? maxResults: null;
      query['projection'] = projection;
      query['fields'] = selector;
    } catch (e) {
      return new Stream.fromFuture(new Future.error(e));
    }
    return _streamJsonRpc("/b",method: "GET",query: query)
        .map((response) => new StorageBucket._(response.right, selector: selector));
  }

  Future<StorageBucket> createBucket(
      var /* String | StorageBucket */ bucket,
      { String projection: 'noAcl',
        String selector: "*"}) {
    return new Future.sync(() {
      var selector;
      if (bucket is String) {
        bucket = {'name' : bucket};
        selector = '*';
      } else if (bucket is! StorageBucket) {
        throw new ArgumentError('Expected String or StorageBucket: $bucket');
      }
      return new _Query(projectId)
          ..['projection'] = projection
          ..['fields'] = selector;
    })
    .then((query) => _sendJsonRpc("/b", method: "POST", query: query, body: bucket.toJson()))
    .then((response) => new StorageBucket._(response, selector: selector));
  }

  Future deleteBucket(
      var /* String | StorageBucket */ bucket,
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch
      }) {
    return new Future.sync(() {
      if (bucket is StorageBucket) {
        bucket = bucket.name;
      } else if (bucket is! String) {
        throw new ArgumentError('Expected String or StorageBucket: $bucket');
      }
      return  new _Query(projectId)
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch;
    }).then((query) => _sendJsonRpc("/b/${bucket}", method: "DELETE", query: query));
  }

  /**
   * Update a bucket, modifying only fields which are selected by the [:readSelector:].
   *
   * Implements a `read, modify, update` loop. First, the bucket metadata is fetched
   * using the [:readSelector:]. The bucket metatadata can be modified using the [:modify:]
   * function, and then exactly those fields which were selected by the [:readSelector:] will
   * be updated on the bucket.
   *
   * [:ifMetagenerationMatch:] will make the return of the bucket's metadata conditional on
   * whether the bucket's current [:metageneration:] matches the value
   *
   * [:ifMetagenerationNotMatch:] will make the return of the bucket's metadata conditional on
   * whether the bucket's current [:metageneration:] does not match the value.
   *
   * [:projection:] determines whether [:acl:] and [:defaultObjectAcl:] properties are selected
   * on the returned bucket.
   * Valid values are `"full"` and `"noAcl"`. Defaults to `"noAcl"`.
   * In order to fetch the [:acl:] properties, the connection to cloud storage must be made by
   * a project [OWNER].
   *
   * [:resultSelector:] determines the partial response which is populated bucket metadata returned by the
   * rpc. Must be a valid [Selector] string.
   *
   * eg. To update the [:websiteConfiguration:] of the bucket [:example:]
   *
   *     connection.patchBucket('example', 'websiteConfiguration',
   *        (bucket) {
   *          bucket.website.mainPageSuffix = 'index.html';
   *          //Setting the value to `null` will clear the value of the field.
   *          bucket.website.notFoundPage = null;
   *        },
   *        resultSelector: 'name,websiteConfiguration');
   */
  Future<StorageBucket> updateBucket(
      String name,
      String readSelector,
      StorageBucket modify(StorageBucket bucket),
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection,
        String resultSelector: "*"}) {
    return new Future.sync(() {
      if (projection != null) {
        // FIXME: See https://developers.google.com/storage/docs/json_api/v1/buckets/patch
        // Projection only works on patch if overridden with the value 'full'

        projection = 'full';
      }
      Map<String,dynamic> modifyJson(Map<String,dynamic> json) {
        return modify(new StorageBucket._(json, selector: readSelector)).toJson();
      }
      var query = new _Query(projectId)
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['projection'] = projection
          ..['fields'] = readSelector;
      return _readModifyUpdate("/b/$name", query, modifyJson, resultSelector)
          .then((response) => new StorageBucket._(response, selector: resultSelector));
    });
  }

  Future<Map<String,dynamic>> _readModifyUpdate(
      String path,
      _Query query,
      Map<String,dynamic> modifyJson(Map<String,dynamic> json),
      String resultSelector) {
    return _sendJsonRpc(path, method: "GET", query: query)
        .then(modifyJson)
        .then((modified) {
          query = new _Query.from(query)
              ..['fields'] = resultSelector;
          return _sendJsonRpc(path, method: "PATCH", query: query, body: modified);
        });
  }


  /**
   * Lists all [StorageObject]s in a bucket.
   * If [:maxResults:] is a positive integer, represents the maximum number of results to fetch
   * in the request.
   * If [:versions:] is `true`, different versions of the same [StorageObject] will be returned
   * as separate items in the [Stream].
   * [:selector:] is a selector to be applied to the results. Only paths in the selector will
   * be populated in the returned object.
   */
  Stream<StorageObject> listBucket(
      var bucket,
      { int maxResults: -1,
        bool versions: false,
        String selector: "*"
      }) {
    return listBucketContents(bucket, null, maxResults: maxResults, delimiter: null, versions: versions, selector: selector)
        .map((content) => content.right);
  }

  /**
   * A directory like listing of all contents in the [:bucket:].
   * [:prefix:] filters all objects in the bucket whose name starts with the specified
   * prefix.
   * [:delimiter:] is the directory separator of the bucket. Defaults to `'/'`.
   * [:maxResults:] is the maximum number of results to return in the request.
   * If [:versions:] is `true`, separate versions of the same object will be
   * emitted as separate items in the returned [Stream].
   * [:selector:] is a selector to apply to [StorageObject]s.
   *
   *
   * Returns a [Stream] of [Either], where each element is:
   * - A right value if the object's name matches `'^$prefix([^$delimiter])*\$`
   * - A left value if the object's name matches `'^$prefix([^$delimiter])+$delimiter.*'`
   *   In this case, the inner value of the match will contain the object's name up
   *   until the first delimiter after the prefix
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
   * To list all [StorageObject]s in a bucket without virtual folders, see [:listBucket:]
   */
  Stream<Either<String,StorageObject>> listBucketContents(
      var bucket, String prefix,
      { int maxResults: -1,
        String projection: 'noAcl',
        String delimiter: "/",
        bool versions: false,
        String selector: "*"
      }) {
    var query;
    try {
      if (bucket is StorageBucket) {
        bucket = bucket.name;
      } else if (bucket is! String) {
        return new Stream.fromFuture(new Future.error("Expected either a String or a StorageBucket"));
      }
      query = new _Query(projectId)
          ..['maxResults'] = (maxResults >= 0) ? maxResults : null
          ..['projection'] = projection
          ..['delimiter'] = delimiter
          ..['versions'] = versions
          ..['prefix'] = prefix
          ..['fields'] = selector;
    } catch (e) {
      return new Stream.fromFuture(new Future.error(e));
    }

    return _streamJsonRpc("/b/$bucket/o", method: "GET", query: query)
        .map((result) => result.map((json) => new StorageObject._(json, selector: selector)));
  }

  Future<StorageObject> getObject(
      var /* String | StorageBucket */ bucket,
      String name,
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String selector: "*"
      }) {
    return new Future.sync(() {
      if (bucket is StorageBucket) {
        bucket = bucket.name;
      } else if (bucket is! String) {
        throw new ArgumentError("Expected either a String or a StorageBucket");
      }
      return new _Query(projectId)
          ..['fields'] = selector
          ..['projection'] = projection
          ..['generation'] = generation
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch;
    })
    .then((query) => _sendJsonRpc("/b/$bucket/o/$name", method: "GET", query: query))
    .then((response) => new StorageObject._(response, selector: selector));
  }

  Future<StorageObject> updateObject(
      String bucket,
      String object,
      StorageObject modify(StorageObject object),
      String readSelector,
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String resultSelector: "*"
      }) {
    return new Future.sync(() {
      var query = new _Query(projectId)
          ..['generation'] = generation
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['projection'] = projection
          ..['fields'] = readSelector;
      modifyJson(Map<String,dynamic> json) {
        return modify(new StorageObject._(json, selector: readSelector)).toJson();
      }
      return _readModifyUpdate("/b/$bucket/o/$object", query, modifyJson, resultSelector);
    })
    .then((response) => new StorageObject._(response, selector: resultSelector));
  }


  /**
   * Uploads an object to a bucket.
   * Suitable for objects up to `5MB` which can be reuploaded if the upload fails.
   *
   */
  Future<StorageObject> uploadObject(
      var /* String | StorageBucket */ bucket,
      var /* String | StorageObject */ object,
      String mimeType,
      List<int> uploadData,
      { int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String selector: "*"
      }) {
    return new Future.sync(() {
      if (bucket is StorageBucket) {
        bucket = bucket.name;
      } else if (bucket is! String) {
        throw new ArgumentError("Expected a String or StorageBucket");
      }
      if (object is String) {
        object = { 'bucket': bucket, 'name': object };
      } else if (object is! StorageObject) {
        throw new ArgumentError("Expected a String or StorageObject");
      }
      return new _Query(projectId)
          ..['uploadType'] = 'multipart'
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifGenerationNotMatch'] = ifGenerationNotMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch
          ..['fields'] = selector;
    })
    .then((query) {

      var request = new http.Request("POST", platformUploadUrl("/b/$bucket/o", query: query));
      request.headers[HttpHeaders.CONTENT_TYPE] = _MULTIPART_CONTENT;

      BytesBuilder bytes = new BytesBuilder();

      void addMultipartContent(Map<String,String> partHeaders, List<int> partBody) {
        bytes..add(_MULTIPART_CONTENT_SEPARATOR)..add(_NEWLINE);
        partHeaders.forEach((k,v) {
          bytes..add(UTF8.encode('$k: '))..add(UTF8.encode(v))..add(_NEWLINE);
        });
        bytes.add(_NEWLINE);
        bytes.add(partBody);
        bytes..add(_NEWLINE)..add(_NEWLINE);
      }

      //Add the object's metadata to the request
      addMultipartContent(
          { HttpHeaders.CONTENT_TYPE: _JSON_CONTENT },
           UTF8.encode(JSON.encode(object)));
      //Add the upload data to the request
      addMultipartContent(
          { HttpHeaders.CONTENT_TYPE: mimeType },
          uploadData);

      bytes.add(_MULTIPART_CONTENT_TERMINATOR);

      request.bodyBytes = bytes.toBytes();
      return _sendAuthorisedRequest(request);
    })
    .then(http.Response.fromStream)
    .then(_handleJsonResponse)
    .then((response) => new StorageObject._(response, selector: selector));
  }

  //TODO: Implement resumable uploads
  // https://developers.google.com/storage/docs/json_api/v1/how-tos/upload
  Future<Either<ResumeToken,StorageObject>> resumableUploadObject(
      var /* String | StorageBucket */ bucket,
      var /* String | StorageObject */ object,
      String mimeType,
      StreamSink<List<int>> uploadData,
      { int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String selector: "*"
      }) {
    throw new UnimplementedError("connection.resumableUpload");
  }

  Future deleteObject(
      var /* String | StorageBucket */ bucket,
      var /* String | StorageObject */ object,
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch
      }) {
    return new Future.sync(() {
      if (bucket is StorageBucket) {
        bucket = bucket.name;
      } else if (bucket is! String) {
        throw new ArgumentError("Expected String or StorageBucket");
      }
      if (object is StorageObject) {
        object = object.name;
      } else if (object is! String) {
        throw new ArgumentError("Expected String or StorageObject");
      }
      return new _Query(projectId)
          ..['generation'] = generation
          ..['ifGenerationMatch'] = ifGenerationMatch
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] = ifMetagenerationNotMatch;
    })
    .then((query) => _sendJsonRpc("/b/$bucket/o/$object", method: "DELETE", query:query));
  }

  /**
   * If [:sourceObject
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
        String selector: "*"
      }) {
    return new Future.sync(() {
      return new _Query(projectId)
        ..['sourceGeneration'] = sourceGeneration
        ..['ifSourceGenerationMatch'] = ifSourceGenerationMatch
        ..['ifSourceGenerationNotMatch'] = ifSourceGenerationNotMatch
        ..['ifSourceMetagenerationMatch'] = ifSourceMetagenerationMatch
        ..['ifSourceMetagenerationNotMatch'] = ifSourceMetagenerationNotMatch
        ..['ifGenerationMatch'] = ifDestinationGenerationMatch
        ..['ifGenerationNotMatch'] = ifDestinationGenerationNotMatch
        ..['ifMetagenerationMatch'] = ifDestinationMetagenerationMatch
        ..['ifMetagenerationNotMatch'] = ifDestinationMetagenerationNotMatch
        ..['projection'] = projection
        ..['fields'] = selector;
    })
    .then((query) {
      var path = "/b/$sourceBucket/o/$sourceObject/copyTo/b/$destinationBucket";
      if (destinationObject is String) {
        //use the source bucket's metadata
        return _sendJsonRpc("$path/${destinationObject}", method: "POST", query: query);
      } else {
        //Use the metadata provided by the destination bucket
        return _sendJsonRpc("$path/${destinationObject.name}", method: "POST", query: query, body: destinationObject.toJson());
      }
    })
    .then((response) => new StorageObject._(response, selector: selector));
  }

  /**
   * Concatenate the given source objects (all in the same bucket) into a single
   * object in the same bucket.
   *
   * [:sourceGenerations:] is a list of generations, which, if provided, must be in one-to-one
   * correspondence with the source objects. Each generation specifies the generation of the corresponding
   *
   */
  Future<StorageObject> composeObjects(
      List</*String | CompositionSource */ dynamic> sourceObjects,
      String destinationBucket,
      String destinationObject,
      { int ifGenerationMatch,
        int ifMetagenerationMatch,
        String selector: "*"
      }) {
    return new Future.sync(() {
      return new _Query(projectId)
        ..['ifGenerationMatch'] = ifGenerationMatch
        ..['ifMetagenerationMatch'] = ifMetagenerationMatch
        ..['fields'] = selector;
    })
    .then((query) {
      var body = { 'kind': 'storage#composeRequest', 'sourceObjects': [] };
      body['destination'] = { 'bucket': destinationBucket, 'name': destinationObject };

      for (var obj in sourceObjects) {
        if (obj is String) {
          body['sourceObjects'].add({'name':obj});
        } else if (obj is CompositionSource) {
          body['sourceObjects'].add(obj.toJson());
        } else {
          throw new ArgumentError("Invalid source object. Should be a list of String or CompositionSource objects");
        }
      }
      return _sendJsonRpc("/b/$destinationBucket/o/${destinationObject}", method: "POST", query: query, body: body);
    })
    .then((response) => new StorageObject._(response, selector: selector));

  }


}


class CompositionSource {

  /**
   * The name of the object.
   */
  final String name;
  /**
   * The generation of the source object to use during composition.
   * If not provided, the latest existing generation of the object is used.
   */
  final int generation;

  /**
   * Only perform the composition if the generation of the source matches
   * the given value.
   *
   * If both [:ifGenerationMatch:] and [:generation:] are provided,
   * the operation will fail if the two values are not the same.
   */
  final int ifGenerationMatch;

  CompositionSource(this.name, {this.generation, this.ifGenerationMatch});


  toJson() {
    var json = { 'name' : name };
    if (generation != null)
      json['generation'] = generation;
    if (ifGenerationMatch != null)
      json['objectPreconditions'] = {'ifGenerationMatch': ifGenerationMatch };
    return json;
  }
}

class ResumeToken {
  //TODO: Implement ResumeToken
}


class RPCException implements Exception {
  final http.Response response;
  final String method;
  final url;
  final String message;

  RPCException(this.response, this.method, this.url, [String this.message]);

  RPCException.invalidStatus(response, method, url):
    this(response, method, url, response.body);

  RPCException.expectedJSON(response, method, url):
    this(response, method, url,
        "Expected JSON response, got ${response.headers[HttpHeaders.CONTENT_TYPE]}");

  RPCException.expectedGzip(response, method, url):
    this(response, method, url,
        "Expected GZIP encoded response, got ${response.headers[HttpHeaders.CONTENT_ENCODING]}");

  String toString() =>
      "Request to remote procedure call $method failed with status ${response.statusCode}\n"
      "endpoint: $url\n"
      "message: ${message}";
}


class _Query extends DelegatingMap<String,String> {
  _Query(String projectId): super({'project': projectId});

  _Query.from(_Query query) : super(new Map.from(query));

  @override
  void addAll(Map<String,dynamic> other) {
    other.forEach((k,v) => this[k] = v);
  }

  @override
  void operator []=(String key, dynamic value) {
    if (value == null) return;
    if (key == "projection") {
      if (!['full', 'noacl'].contains(value.toLowerCase())) {
        throw new ArgumentError(
            'Invalid value for projection: ${value}'
            'Valid values are \'full\', \'noAcl\'');
      }
    }
    super[key] = value.toString();
  }

  @override
  String toString() {
    StringBuffer sbuf = new StringBuffer();
    forEach((k,v) {
      if (sbuf.isNotEmpty) sbuf.write("&");
      sbuf.write("$k=$v");
    });
    return sbuf.toString();
  }
}


