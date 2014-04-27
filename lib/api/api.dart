library google_cloud_storage.api;

import 'dart:async';
import 'dart:convert' show JSON, UTF8;
import 'dart:io';
import 'package:http/http.dart' as http;

import 'package:google_oauth2_client/google_oauth2_console.dart' as oauth2;

import 'package:quiver/async.dart';

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

  Map<String,String> get _initQuery => { 'project': projectId };

  /**
   * Get the platform url to submit a request.
   * - [:path:] is the path to the resource (eg. /b/<bucket>
   * - [:query:] is the parameters to pass to the url.
   * - [:apiBaseUrl:] The base url of the api endpoint
   * - [:apiVersion:] The version of the API to call.
   *
   * Returns the API endpoint url.
   */
  Uri platformUrl(String path,{ Map<String,String> query }) {
    var q = query.keys.map((k) => "$k=${query[k]}").join("&");
    return Uri.parse("$API_ENDPOINT/storage/${API_VERSION}${path}?${q}");
  }

  /**
   * Submits a `JSON` RPC call to the cloud storage service.
   *
   */
  Future<Map<String,dynamic>> _sendJsonRpc(
      String path,
      { String method: "GET",
        Map<String,String> query,
        Map<String,dynamic> body
      }) {
    assert(query != null);

    var projection = query['projection'];
    if (projection != null) {
      if (!['full', 'noacl'].contains(projection.toLowerCase()))
        return new Future.error('Invalid value for projection: ${projection}');
      if (role != PermissionRole.OWNER)
        return new Future.error(
            'Insufficient permissions for projection; '
            'Connection must be made with the OWNER permission role'
        );
    }

    var url = platformUrl(path, query: query);
    http.Request request = new http.Request(method, url);

    if (!["GET", "DELETE"].contains(method)) {
      var contentType = new ContentType('application', 'json', charset: 'UTF-8');
      request.headers[HttpHeaders.CONTENT_TYPE] = contentType.toString();
      print(request.headers);
      request.bodyBytes = UTF8.encode(JSON.encode(body));
    }
    return _sendAuthorisedRequest(request)
        .then(http.Response.fromStream)
        .then(
    (http.Response response) {
      if (response.statusCode < 200 || response.statusCode >= 300)
        throw new RPCException.invalidStatus(response, method, url);
      if (response.statusCode == HttpStatus.NO_CONTENT) {
        assert(method == "DELETE");
        return null;
      }
      var contentType = ContentType.parse(response.headers[HttpHeaders.CONTENT_TYPE]);

      if (contentType.primaryType != 'application' || contentType.subType != 'json')
        throw new RPCException.expectedJSON(response, method, url);

      return JSON.decode(UTF8.decode(response.bodyBytes));
    });
  }

  /**
   *
   */
  Stream<Map<String,dynamic>> _streamJsonRpc(
      String path,
      { String method: "GET",
        Map<String,dynamic> query,
        Map<String,dynamic> body
      }) {
    StreamController<Map<String,dynamic>> controller = new StreamController();
    if (query['fields'] != "*") {
      query['fields'] = 'nextPageToken,items(${query['fields']})';
    } else {
      query['fields'] = '*';
    }

    // Add the next page of the results (starting at `nextPageToken`)
    // to the controller.
    Future addNextPage(String nextPageToken) {
      var pageQuery = new Map.from(query);
      pageQuery['pageToken'] = nextPageToken;
      return _sendJsonRpc(path,
          method: method,
          query: new Map.from(query),
          body: body)
      .then((response) {
        for (var item in response['items']) {
          if (!controller.isClosed) controller.add(item);
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
    var query = _initQuery;

    if (ifMetagenerationMatch != null)
      query['ifMetagenerationMatch'] = '$ifMetagenerationMatch';
    if (ifMetagenerationNotMatch != null)
      query['ifMetagenerationNotMatch'] = '$ifMetagenerationNotMatch';

    query['projection'] = projection;
    query['fields'] = selector;

    return _sendJsonRpc("/b/$bucket", method: "GET", query:query).then((response) {
      return new StorageBucket._(response, selector: selector);
    });
  }

  Stream<StorageBucket> listBuckets(
      {int maxResults: -1,
       String projection: 'noAcl',
       String selector: "*"}) {
    var query = _initQuery;

    query['projection'] = projection;
    query['fields'] = selector;
    if (maxResults >= 0)
      query['maxResults'] = '$maxResults';

    return _streamJsonRpc("/b",method: "GET",query: query)
        .map((response) => new StorageBucket._(response, selector: selector));

  }

  Future<StorageBucket> createBucket(
      var /* String | StorageBucket */ bucket,
      {String projection: 'full'}) {

    var selector;
    if (bucket is String) {
      bucket = {'name' : bucket};
      selector = '*';
    } else if (bucket is StorageBucket) {
      selector = bucket.selector;
    } else {
      throw 'Expected String or StorageBucket: $bucket';
    }

    var query = _initQuery;

    query['projection'] = projection;

    query['fields'] = bucket.selector;
    return _sendJsonRpc("/b", method: "POST", query: query, body: bucket.toJson()).then((response) {
      return new StorageBucket._(response, selector: selector);
    });
  }

  Future deleteBucket(
      var /* String | StorageBucket */ bucket,
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch
      }) {

    if (bucket is StorageBucket) {
      bucket = bucket.name;
    } else if (bucket is! String) {
      throw 'Expected String or StorageBucket: $bucket';
    }

    var query = {};
    if (ifMetagenerationMatch != null)
      query['ifMetagenerationMatch'] = '$ifMetagenerationMatch';
    if (ifMetagenerationNotMatch != null)
      query['ifMetagenerationNotMatch'] = '$ifMetagenerationNotMatch';
    return _sendJsonRpc("/b/${bucket}", method: "DELETE", query: query);
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
   *        }
   *
   *
   */
  Future<StorageBucket> updateBucket(
      String name,
      String readSelector,
      StorageBucket modify(StorageBucket bucket),
      { int ifMetagenerationMatch,
        int ifMetagenerationNotMatch,
        String projection,
        String resultSelector: "*"}) {
    var query = _initQuery;
    if (ifMetagenerationMatch != null)
      query['ifMetagenerationMatch'] = '$ifMetagenerationMatch';
    if (ifMetagenerationNotMatch != null)
      query['ifMetagenerationNotMatch'] = '$ifMetagenerationNotMatch';

    if (projection != null) {
      // FIXME: See https://developers.google.com/storage/docs/json_api/v1/buckets/patch
      // Projection only works on patch if overridden with the value 'full'
      assert(projection == 'full');
      query['projection'] = projection;
    }
    query['fields'] = readSelector;

    return _sendJsonRpc("/b/$name", method: "GET", query: query)
        .then((response) => modify(new StorageBucket._(response, selector: readSelector)))
        .then((bucket) {
          query['fields'] = resultSelector;
          return _sendJsonRpc("/b/$name", method: "PATCH", query: query, body: bucket.toJson());
        })
        .then((response) => new StorageBucket._(response, selector: resultSelector));
  }
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


