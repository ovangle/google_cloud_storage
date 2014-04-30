library google_cloud_storage.api;

import 'dart:async';
import 'dart:convert' show JSON, UTF8;
import 'dart:io';
import 'dart:math' as math;

import 'package:http/http.dart' as http;
import 'package:collection/wrappers.dart';

import 'package:google_oauth2_client/google_oauth2_console.dart' as oauth2;

import '../either/either.dart';
import '../json/object.dart';

part 'src/access_controls.dart';
part 'src/entry.dart';
part 'src/misc.dart';


const API_VERSION = 'v1beta2';

const API_BASE_URL = 'https://www.googleapis.com';

final _API_BASE = Uri.parse(API_BASE_URL);

const API_SCOPES =
    const { PermissionRole.READER : 'https://www.googleapis.com/auth/devstorage.read_only',
            PermissionRole.WRITER : 'https://www.googleapis.com/auth/devstorage.read_write',
            PermissionRole.OWNER: 'https://www.googleapis.com/auth/devstorage.full_control'
          };

const _JSON_CONTENT = 'application/json; charset=UTF-8';
const _MULTIPART_CONTENT = 'multipart/related; boundary=content_boundary';

typedef Future<dynamic> _ResponseHandler(http.Response response, int maxRetries, [int currRetry]);

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

final _random = new math.Random();

/**
 * The Http status codes to use when retrying a request automatically
 */
const _RETRY_STATUS =
      const [ HttpStatus.REQUEST_TIMEOUT,
              HttpStatus.INTERNAL_SERVER_ERROR,
              HttpStatus.BAD_GATEWAY,
              HttpStatus.SERVICE_UNAVAILABLE,
              HttpStatus.GATEWAY_TIMEOUT
            ];


Future _readPrivateKey(String path) {
  if (path == null)
    return new Future.value();
  return new File(path).readAsString();
}

class CloudStorageConnection {
  static Future<CloudStorageConnection> open(String projectNumber, String projectId, PermissionRole role,
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
      _sendAuthorisedRequest(http.BaseRequest request) =>
          console.withClient(
              (client) => client.send(request));
      return new CloudStorageConnection._(projectId, role, _sendAuthorisedRequest);
    });
  }

  final String projectId;
  /**
   * The role of the user who established this connection.
   */
  final PermissionRole role;
  final _sendAuthorisedRequest;

  CloudStorageConnection._(this.projectId, this.role, Future<http.StreamedResponse> this._sendAuthorisedRequest(http.BaseRequest request));

  /**
   * Get the platform url to submit a request.
   * - [:path:] is a urlencoded path to the API endpoint,
   * specified relative to `https://www.googleapis.com/storage/v1beta2`
   * - [:query:] is a _Query object which specifies the parameters
   * to the api endpoint.
   *
   * Returns the API endpoint url.
   */
  Uri _platformUrl(String path,{ _Query query }) =>
      Uri.parse("$API_BASE_URL/storage/${API_VERSION}${path}?${query}");

  /**
   * Gets the platform url to submit an upload request.
   * - [:path:] is a urlencoded path to the API endpoint,
   * specified relative to `https://www.googleapis.com/upload/storage/v1beta2`
   * - [:query:] is a [_Query] object which specifies parameters
   * to pass to the API endpoint.
   *
   * Returns the API endpoint url.
   */
  Uri platformUploadUrl(String path, { _Query query}) =>
      Uri.parse("$API_BASE_URL/upload/storage/${API_VERSION}${path}?${query}");

  /**
   * Submits a remote procedure call against one of the google cloud storage API endpoints.
   *
   * - [:path:] is a urlencoded path to the request endpoint,
   * specified relative to `https://www.googleapis.com/storage/v1beta2`
   * - [:query:] is a [_Query] object containing parameters to pass to the remote procedure
   * call.
   * - [:handler:] is a [_ResponseHandler] to pass the
   * - [:body:] is the body of the request, which must be to a JSON encodable object.
   * - [:maxRetries:] is the maximum number of attempts to submit the server (with exponential
   * backoff) before failing the request when the cloud storage server returns any of the
   * responses:
   *   - request timeout
   *   - internal server error
   *   - bad gatway
   *   - service unavailable
   *   - gateway timeout
   *
   */
  Future<dynamic> _sendJsonRpc(
      String path,

      { String method: "GET",
        _ResponseHandler handler,
        _Query query,
        var body,
        int maxRetries: 0
      }) {
    if (handler == null) handler = _handleJsonResponse;
    assert(query != null);
    print(query);

    var url = _platformUrl(path, query: query);
    http.Request request = new http.Request(method, url);

    if (!["GET", "DELETE"].contains(method)) {
      request.headers[HttpHeaders.CONTENT_TYPE] = _JSON_CONTENT;
      request.bodyBytes = UTF8.encode(JSON.encode(body));
    }
    return _sendAuthorisedRequest(request)
        .then(http.Response.fromStream)
        .then((response) => handler(response, maxRetries));
  }

  /**
   * A response handler for any google cloud storage request which returns a JSON encoded response.
   * Checks the response body
   */
  Future<Map<String,dynamic>> _handleJsonResponse(http.Response response, int maxRetries, [int retry=0]) =>
      _handleResponse(response, maxRetries, retry)
      .then((result) {
          if (response.statusCode == HttpStatus.NO_CONTENT) {
            assert(response.request.method == "DELETE");
            return null;
          }

          var contentType = ContentType.parse(response.headers[HttpHeaders.CONTENT_TYPE]);

          if (contentType.primaryType != 'application' || contentType.subType != 'json')
            throw new RPCException.expectedJSON(response);

          return JSON.decode(response.body);
        });

  Future<dynamic> _handleResponse(http.Response response, int maxRetries, [int retry=0]) {
    //Resend the remote procedure call, with a delay calculated as ( 2^retry seconds + <random> microseconds )
    Future<dynamic> _resendRpcWithDelay() {
      Duration retryDuration = new Duration(seconds: math.pow(2, retry), milliseconds: _random.nextInt(1000));
      return new Future.delayed((retryDuration), () {
        _sendAuthorisedRequest(response.request)
            .then(http.Response.fromStream)
            .then((response) => _handleResponse(response, maxRetries, retry + 1));
      });
    }

    if (_RETRY_STATUS.contains(response.statusCode) &&
        retry < maxRetries) {
      return _resendRpcWithDelay();
    }

    return new Future.sync(() {
      if (response.statusCode < 200 || response.statusCode >= 300)
        throw new RPCException.invalidStatus(response);


      return response.body;
    });
  }




  /**
   * Submit the json remote procedure call, which returns paged results
   * with paths:
   *
   * - *nextPageToken* A token representing the next page in the result set
   * - *prefixes* An (optional) list of initial portions of object resource names
   * - *items* A list of json resources.
   *
   * Returns a [Stream] of [Either] object, where all prefixes are emitted as left values
   * and resources are submitted as right values.
   *
   * [:path:] is the path to the request endpoint, specified relative to the google
   * storage api base url
   * [:query:] is a [_Query] object containing parameters to pass to the remote procedure
   * call.
   * [:body:] is the body of the request.
   * [:maxTries:] is the maximum number of retries to submit against the server *when
   * fetching each page*, if the server returns any of the statuses
   *   - request timeout
   *   - internal server error
   *   - bad gatway
   *   - service unavailable
   *   - gateway timeout
   *
   */
  Stream<Either<String,Map<String,dynamic>>> _streamJsonRpc(
      String path,
      { String method: "GET",
        _Query query,
        Map<String,dynamic> body,
        int maxRetries: 0
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

      return _sendJsonRpc(
          path,
          method: method,
          query: pageQuery,
          body: body,
          maxRetries: maxRetries)
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
        int ifMetagenerationNotMatch,
        int maxRetries: 0}) {
    return new Future.sync(() =>
        new _Query(projectId)
          ..['ifMetagenerationMatch'] = ifMetagenerationMatch
          ..['ifMetagenerationNotMatch'] =ifMetagenerationNotMatch
          ..['projection'] = projection
          ..['fields'] = selector
    )
    .then((query) => _sendJsonRpc("/b/$bucket", method: "GET", query:query, maxRetries: maxRetries))
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
    return _streamJsonRpc("/b",method: "GET",query: query, maxRetries: 1)
        .map((response) => new StorageBucket._(response.right, selector: selector));
  }

  Future<StorageBucket> createBucket(
      var /* String | StorageBucket */ bucket,
      { String projection: 'noAcl',
        String selector: "*",
        int maxRetries: 0}) {
    return new Future.sync(() {
      var selector;
      if (bucket is String) {
        _checkValidBucketName(bucket);
        bucket = {'name' : bucket};
        selector = '*';
      } else if (bucket is! StorageBucket) {
        _checkValidBucketName(bucket.name);
        throw new ArgumentError('Expected String or StorageBucket: $bucket');
      }
      return new _Query(projectId)
          ..['projection'] = projection
          ..['fields'] = selector;
    })
    .then((query) => _sendJsonRpc("/b", method: "POST", query: query, body: bucket, maxRetries: maxRetries))
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
      void modify(StorageBucket bucket),
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection,
        String readSelector: "*",
        String resultSelector: "*"}) {
    return new Future.sync(() {
      if (projection != null) {
        // FIXME: See https://developers.google.com/storage/docs/json_api/v1/buckets/patch
        // Projection only works on patch if overridden with the value 'full'

        projection = 'full';
      }
      void modifyJson(Map<String,dynamic> json) {
        modify(new StorageBucket._(json, selector: readSelector));
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
      void modifyJson(Map<String,dynamic> json),
      String resultSelector) {
    return _sendJsonRpc(path, method: "GET", query: query)
        .then((json) {
          modifyJson(json);
          query = new _Query.from(query)
              ..['fields'] = resultSelector;
          return _sendJsonRpc(path, method: "PATCH", query: query, body: json);
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
    .then((query) => _sendJsonRpc("/b/$bucket/o/${_urlEncode(name)}", method: "GET", query: query))
    .then((response) => new StorageObject._(response, selector: selector));
  }

  Future<StorageObject> updateObject(
      String bucket,
      String object,
      void modify(StorageObject object),
      { int generation,
        int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String readSelector: "*",
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
      void modifyJson(Map<String,dynamic> json) {
        return modify(new StorageObject._(json, selector: readSelector));
      }
      return _readModifyUpdate("/b/$bucket/o/${_urlEncode(object)}", query, modifyJson, resultSelector);
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
        String selector: "*",
        int maxRetries: 0
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
    .then((response) => _handleJsonResponse(response, maxRetries))
    .then((response) => new StorageObject._(response, selector: selector));
  }

  //TODO: Implement resumable uploads
  // https://developers.google.com/storage/docs/json_api/v1/how-tos/upload
  Future<Either<ResumeToken,StorageObject>> resumableUploadObject(
      var /* String | StorageBucket */ bucket,
      var /* String | StorageObject */ object,
      String mimeType,
      Stream<List<int>> uploadData,
      { int ifGenerationMatch,
        int ifGenerationNotMatch,
        int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection: 'noAcl',
        String selector: "*"
      }) {
    //TODO: Check valid object name.
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
    .then((query) => _sendJsonRpc("/b/$bucket/o/${_urlEncode(object)}", method: "DELETE", query:query));
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
      var path = "/b/$sourceBucket/o/${_urlEncode(sourceObject)}/copyTo/b/$destinationBucket";
      if (destinationObject is String) {
        _checkValidObjectName(destinationObject);
        //use the source bucket's metadata
        return _sendJsonRpc("$path/${_urlEncode(destinationObject)}", method: "POST", query: query);
      } else {
        _checkValidObjectName(destinationObject.name);
        //Use the metadata provided by the destination bucket
        return _sendJsonRpc("$path/${_urlEncode(destinationObject.name)}", method: "POST", query: query, body: destinationObject);
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
      _checkValidObjectName(destinationObject);
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
      return _sendJsonRpc("/b/$destinationBucket/o/${_urlEncode(destinationObject)}", method: "POST", query: query, body: body);
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
  final String message;

  int get statusCode => response.statusCode;
  String get method => response.request.method;
  Uri get url => response.request.url;

  RPCException(this.response, [String this.message]);

  RPCException.invalidStatus(response):
    this(response, response.body);

  RPCException.expectedJSON(response):
    this(response, "Expected JSON response, got ${response.headers[HttpHeaders.CONTENT_TYPE]}");

  String toString() =>
      "Request to remote procedure call $method failed with status ${response.statusCode}\n"
      "endpoint: $url\n"
      "message: ${message}";
}

final _BUCKET_NAME = new RegExp(r'^[a-z0-9]([a-zA-Z0-9_.-]+)[a-z0-9]$');
final _IP = new RegExp(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$');

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

void _checkValidObjectName(String name) {
  if (name == "")
    throw new ArgumentError("Object name cannot be empty");
  if (UTF8.encode(name).length > 1024)
    throw new ArgumentError("Object names are limited to 1024 bytes when encoded as a UTF-8 string");
  if (name.contains('\n') || name.contains('\r'))
    throw new ArgumentError("Object name cannot contain newline ('\n') or carriage return ('\r') characters");
}

const _urlEncode = Uri.encodeComponent;


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
      sbuf.write("$k=${_urlEncode(v)}");
    });
    return sbuf.toString();
  }
}


