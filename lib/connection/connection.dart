library connection;

import 'dart:async';
import 'dart:convert' show UTF8, JSON;
import 'dart:io';
import 'dart:math' as math;

import 'package:collection/wrappers.dart' show DelegatingMap;
import 'package:crypto/crypto.dart' show MD5, CryptoUtils;
import 'package:http/http.dart' as http;
import 'package:logging/logging.dart';

import 'package:google_oauth2_client/google_oauth2_console.dart' as oauth2;

import '../api/api.dart';
import '../utils/either.dart';
import '../json/path.dart';
import '../json/selector.dart';

part 'src/bucket_requests.dart';
part 'src/object_requests.dart';

/**
 * Percent escape a url component.
 */
const _urlEncode = Uri.encodeComponent;

const API_VERSION = 'v1';

const API_BASE_URL = 'https://www.googleapis.com';

final _API_BASE = Uri.parse(API_BASE_URL);

const API_SCOPES =
    const { PermissionRole.READER : 'https://www.googleapis.com/auth/devstorage.read_only',
            PermissionRole.WRITER : 'https://www.googleapis.com/auth/devstorage.read_write',
            PermissionRole.OWNER: 'https://www.googleapis.com/auth/devstorage.full_control'
          };

const _JSON_CONTENT = 'application/json; charset=UTF-8';
const _MULTIPART_CONTENT = 'multipart/related; boundary=content_boundary';

typedef Future<dynamic> _ResponseHandler(http.Response response);





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



class CloudStorageConnection extends ConnectionBase
with BucketRequests,
     ObjectRequests {

  static Future<CloudStorageConnection> open(String projectNumber, String projectId, PermissionRole role,
      { String serviceAccount, String pathToPrivateKey}) {

    Future<String> _readPrivateKey(String path) =>
        (path == null)
        ? new Future.value()
        : new File(path).readAsString();

    return _readPrivateKey(pathToPrivateKey).then((privateKey) {
      var scopes;
      if (serviceAccount != null && pathToPrivateKey != null) {
        scopes = [ 'https://www.googleapis.com/auth/userinfo.email',
                   API_SCOPES[role]
                 ].join(" ");
      }
      var console = new oauth2.ComputeOAuth2Console(
          projectNumber,
          iss: serviceAccount,
          privateKey: privateKey,
          scopes: scopes);

      sendAuthorisedRequest(http.BaseRequest request) =>
          console.withClient((client) => client.send(request));

      return new CloudStorageConnection._(projectId, sendAuthorisedRequest);
    });
  }

  CloudStorageConnection._(String projectId, Future<http.StreamedResponse> sendAuthorisedRequest(http.BaseRequest request)):
    super._(projectId, sendAuthorisedRequest);
}


/**
 * An implementation of the basic functionality required
 * to make a remote procedure call against google cloud storage.
 */
abstract class ConnectionBase {

  /**
   * Get the platform url to submit a request.
   * - [:path:] is a urlencoded path to the API endpoint,
   * specified relative to `https://www.googleapis.com/storage/v1beta2`
   * - [:query:] is a _Query object which specifies the parameters
   * to the api endpoint.
   *
   * Returns the API endpoint url.
   */
  Uri _platformUrl(String path, _Query query) =>
      Uri.parse("$API_BASE_URL/storage/${API_VERSION}$path?$query");

  /**
   * Gets the platform url to submit an upload request.
   * - [:path:] is a urlencoded path to the API endpoint,
   * specified relative to `https://www.googleapis.com/upload/storage/v1beta2`
   * - [:query:] is a [_Query] object which specifies parameters
   * to pass to the API endpoint.
   *
   * Returns the API endpoint url.
   */
  Uri _platformUploadUrl(String path, _Query query) =>
      Uri.parse("$API_BASE_URL/upload/storage/${API_VERSION}${path}?${query}");

  /**
   * The google assigned ID of the project which manages the objects mutated
   * by this connection.
   */
  final String projectId;

  /**
   * Send an authorised request to the server, signed with the credentials
   * used to instantiate the connection.
   */
  final _sendAuthorisedRequest;

  /**
   * the number of times to automatically retry a remote procedure call which fails
   * with one of the statuses
   * - `request timeout`
   * - `internal server error`
   * - `bad gateway`
   * - `service unavailable
   * - `gateway timeout`
   *
   * When retrying a request, a delay of `2^(num_retries) seconds + (random microseconds)`
   * is inserted between each successive retry, up to the specified number of retries.
   *
   * The default configured value is `5`.
   */
  int maxRetryRequests = 5;

  /**
   * A logger which handles messages output when performing
   * remote procedure calls on the logger.
   *
   * The default logger name is `cloudstorage.connection`.
   */
  Logger logger = new Logger("cloudstorage.connection");

  ConnectionBase._(this.projectId, Future<http.StreamedResponse> this._sendAuthorisedRequest(http.BaseRequest request));

  /**
   * Submit a remote procedure call to the specified [:path:] with the
   * specified [:query:] parameters.
   */
  Future<dynamic> _remoteProcedureCall(
      String path,
      { String method: "GET",
        Map<String,String> headers: const {},
        _Query query,
        var body,
        _ResponseHandler handler,
        bool isUploadUrl: false
      }) {
    if (handler == null)
      throw new ArgumentError("No handler provided");

    var url;
    if (isUploadUrl) {
      url = _platformUrl(path, query);
    } else {
      url = _platformUploadUrl(path, query);
    }
    http.Request request = new http.Request(method, url);

    var contentType = headers[HttpHeaders.CONTENT_TYPE];
    if (contentType == _JSON_CONTENT && body != null)
      request.bodyBytes = UTF8.encode(JSON.encode(body));

    if (contentType == _MULTIPART_CONTENT) {
      assert(body is List<_MultipartRequestContent>);
      request.bodyBytes = _MultipartRequestContent.encodeBody(body);
    }
    request.headers.addAll(headers);
    var md5Hash = (new MD5()..add(request.bodyBytes)).close();
    request.headers[HttpHeaders.CONTENT_MD5] = md5Hash;

    logger.info("Submitting remote procedure call ($request)");

    return _sendAuthorisedRequest(request)
        .then(http.Response.fromStream)
        .then(handler);
  }

  /**
   * A paged remote procedure is always submitted as a `GET` request
   * to a particular path, which returns a `JSON` map which
   * can contain any of the following keys:
   * - *nextPageToken* A pointer to the next page in the stream
   * - *prefixes* A list of object prefixes
   * - *items* A list of resources.
   * As each page is returns, adds it to a [Stream] as the parsed.
   *
   * The [:query:] must contain a `fields` key, which must specify
   * the fields to filter from the returned pages.
   *
   * Returns a [Stream] where each page is returned as a parsed `JSON` containing
   * the contents of the page.
   */
  Stream<Map<String,dynamic>> _pagedRemoteProcedureCall(
      String path,
      { Map<String,String> headers: const {},
        _Query query
      }) {
    assert(query['fields'] != null);
    var s = Selector.parse(query['fields']);
    if (!s.isPathInSelection(new FieldPath("nextPageToken")))
      throw new ArgumentError("nextPageToken must be in selected fields");

    var pageController = new StreamController<Map<String,dynamic>>();

    Future addNextPage(String nextPageToken) {
      var pageQuery = new _Query.from(query)
          ..['pageToken'] = nextPageToken;
      logger.info("Retrieving paged results results for ($nextPageToken)");
      return _remoteProcedureCall(path, headers: headers, query: pageQuery, handler: _handleJsonResponse)
          .then((json) {
            if (!pageController.isClosed) {

              pageController.add(json);

              var pageToken = json['nextPageToken'];
              if (pageToken != null) {
                return addNextPage(pageToken);
              } else {
                return pageController.close();
              }
            }
          })
          .catchError(pageController.addError);
    }

    addNextPage(null);

    return pageController.stream;
  }

  /**
   * Implements a patch request loop. The result is first fetched and passed to
   * the `readHandler`.
   */
  Future<dynamic> _readModifyPatch(
      String path,
      _Query query,
      Map<String,String> headers,
      void modify(dynamic obj),
      { _ResponseHandler readHandler,
        _ResponseHandler resultHandler,
        String resultSelector
      }) {
    return _remoteProcedureCall(path, query: query, headers: headers, method: "GET", handler: readHandler)
        .then((result) {
          modify(result);
          query['fields'] = resultSelector;
          return _remoteProcedureCall(
              path,
              query: query,
              headers: headers,
              method: "PATCH",
              body: result,
              handler: resultHandler);
        });
  }

  /**
   * A simple response handler.
   * If the status code is one of the status codes we need to retry, then
   * resend the request with an appropriate delay (up to the maximum number
   * of retries). Otherwise, check that the status code is in the 20x range
   * and throw an exception if it isn't.
   */
  Future<dynamic> _handleResponse(http.Response response, [int retryCount=0]) {

    Future<dynamic> resendRpcWithDelay() {
      //The delay is calculated as (2^retryCount + random # of milliseconds)
      Duration delay = new Duration(seconds: math.pow(2, retryCount), milliseconds: _random.nextInt(1000));
      return new Future.delayed(delay, () {
        _sendAuthorisedRequest(response.request)
            .then(http.Response.fromStream)
            .then((response) => _handleResponse(response, retryCount + 1));
      });
    }

    if (_RETRY_STATUS.contains(response.statusCode) &&
        retryCount < maxRetryRequests) {
      logger.warning("Remote procedure call to ${response.request.method} ${response.request.url} failed with status ${response.statusCode}");
      logger.warning("Retrying... (retry count: $retryCount)");
      return resendRpcWithDelay();
    }

    return new Future.sync(() {
      if (response.statusCode < 200 || response.statusCode >= 300) {
        logger.severe("Remote procedure call to ${response.request.url} failed with status ${response.statusCode}");
        throw new RPCException.invalidStatus(response);
      }
      logger.info("Remote procedure call to ${response.request.method} ${response.request.url} returned status ${response.statusCode}");
      return response.body;
    });
  }

  /**
   * Handle a JSON encoded response
   */
  Future<Map<String,dynamic>> _handleJsonResponse(http.Response response) =>
      _handleResponse(response)
      .then((responseBody) {

        var contentType = ContentType.parse(response.headers[HttpHeaders.CONTENT_TYPE]);
        if (contentType.mimeType != "application/json") {
          logger.severe("Expected JSON response from ${response.request}");
          throw new RPCException.expectedJSON(response);
        }

        return JSON.decode(response.body);
      });

  /**
   * Handle a response which is expected to return a NO_CONTENT status.
   */
  Future _handleEmptyResponse(http.Response response) =>
      _handleResponse(response)
      .then((responseBody) {
        if (response.statusCode != HttpStatus.NO_CONTENT) {
          logger.severe("Expected empty response from ${response.request}");
          throw new RPCException.expectedEmpty(response);
        }
        return null;
      });
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


/**
 * Represents an exception which occurred while executing
 * a remote procedure call.
 */
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

  RPCException.expectedEmpty(response):
    this(response, "Expected empty response");

  String toString() =>
      "Request to remote procedure call $method failed with status ${response.statusCode}\n"
      "endpoint: $url\n"
      "message: ${message}";
}

/**
 * Models a parametised query which will be passed via
 * a url to a remote procedure call path.
 */
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

/**
 * A section of a request body with content type `multipart/related`.
 */
class _MultipartRequestContent {
  static const CONTENT_BOUNDARY = 'content_boundary';

  //'\r\n'
  static const NEWLINE = const [0x0D, 0x0A];

  //--content_boundary
  static const MULTIPART_CONTENT_SEPARATOR =
      const [ 0x2D, 0x2D, 0x63, 0x6F, 0x6E, 0x74, 0x65, 0x6E, 0x74, 0x5F,
              0x62, 0x6F, 0x75, 0x6E, 0x64, 0x61, 0x72, 0x79
            ];

  //--content_boundary--
  static const MULTIPART_CONTENT_TERMINATOR =
      const [ 0x2D, 0x2D, 0x63, 0x6F, 0x6E, 0x74, 0x65, 0x6E, 0x74, 0x5F,
              0x62, 0x6F, 0x75, 0x6E, 0x64, 0x61, 0x72, 0x79, 0x2D, 0x2D
            ];

  static List<int> encodeBody(List<_MultipartRequestContent> contentSegments) {
    BytesBuilder builder = new BytesBuilder();
    for (var contentSegment in contentSegments) {
      builder.add(MULTIPART_CONTENT_SEPARATOR);
      contentSegment.addTo(builder);
    }
    builder.add(MULTIPART_CONTENT_TERMINATOR);
    return builder.toBytes();
  }

  /**
   * Headers which apply to this section of the request.
   */
  final Map<String,String> headers = new Map<String,String>();

  List<int> body;

  void addTo(BytesBuilder builder) {

    headers.forEach((k,v) {
      builder..add(UTF8.encode("$k: $v"))..add(NEWLINE);
    });

    builder..add(NEWLINE);
    builder.add(body);

  }
}