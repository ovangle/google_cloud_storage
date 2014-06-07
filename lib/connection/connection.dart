library connection;

import 'dart:async';
import 'dart:convert' show UTF8, JSON;
import 'dart:typed_data';
import 'dart:math' as math;


import 'package:collection/equality.dart' show ListEquality;
import 'package:collection/wrappers.dart' show DelegatingMap;
import 'package:crypto/crypto.dart' show MD5, CryptoUtils;
import 'package:http/http.dart' as http;
import 'package:logging/logging.dart';
import 'package:quiver/async.dart' show forEachAsync;
import 'package:quiver/iterables.dart' show range;

import '../api/api.dart';
import '../source/source_common.dart';
import '../utils/content_range.dart';
import '../utils/either.dart';
import '../utils/http_utils.dart';
import '../json/path.dart';
import '../json/selector.dart';

part 'src/bucket_requests.dart';
part 'src/object_requests.dart';
part 'src/object_transfer_requests.dart';

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

const _LIST_EQ = const ListEquality();

const _JSON_CONTENT = 'application/json; charset=UTF-8';
const _MULTIPART_CONTENT = 'multipart/related; boundary="content_boundary"';

typedef Future<dynamic> _ResponseHandler(_RemoteProcedureCall rpc, http.BaseResponse response);

/**
 * A pseudo random number generator.
 */
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

/**
 * A [Connection] with all the appropriate
 * request mixin classes.
 */

class Connection extends ConnectionBase with
    BucketRequests,
    ObjectRequests,
    ObjectTransferRequests {
  Connection(String projectId, http.BaseClient client):
    super(projectId, client);
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

  ConnectionBase(this.projectId, http.BaseClient client):
    _sendAuthorisedRequest = client.send;

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

    var url =
        (isUploadUrl ? _platformUploadUrl : _platformUrl)(path, query);

    var rpc = new _RemoteProcedureCall(
        (isUploadUrl ? _platformUploadUrl : _platformUrl)(path, query),
        method,
        headers: headers,
        body: body
    );

    return _sendAuthorisedRequest(rpc.asRequest()).then((response) => handler(rpc, response));
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
  Future<http.Response> _handleResponse(_RemoteProcedureCall rpc, http.BaseResponse response, [int retryCount=0]) {

    Future<dynamic> resendRpcWithDelay(http.BaseResponse response, [int retryCount=0]) {
        //The delay is calculated as (2^retryCount + random # of milliseconds)
        Duration delay = new Duration(seconds: math.pow(2, retryCount), milliseconds: _random.nextInt(1000));
        return new Future.delayed(
            delay,
            () => _sendAuthorisedRequest(rpc.asRequest()).then((response) => _handleResponse(rpc, response, retryCount + 1))
        );
    }


    if (_RETRY_STATUS.contains(response.statusCode) &&
        retryCount < maxRetryRequests) {
      logger.warning(
          "Remote procedure call to ${response.request.method} ${response.request.url} "
          "failed with status ${response.statusCode}\n"
          "Retrying... (retry count: $retryCount of $maxRetryRequests)");
      return resendRpcWithDelay(response, retryCount);
    }

    return new Future.sync(() {
      if (response.statusCode < 200 || response.statusCode >= 300) {
        logger.severe("Remote procedure call to ${response.request.url} failed with status ${response.statusCode}");
        throw new RPCException.invalidStatus(response);
      }
      logger.info("Remote procedure call to ${response.request.method} ${response.request.url} returned status ${response.statusCode}");
      return response;
    });
  }

  /**
   * Handle a JSON encoded response
   */
  Future<Map<String,dynamic>> _handleJsonResponse(_RemoteProcedureCall rpc, http.BaseResponse response) =>
      _handleResponse(rpc, response).then((response) {

        var contentType = response.headers[HttpHeaders.CONTENT_TYPE];
        if (!contentType.startsWith("application/json")) {
          logger.severe("Expected JSON response from ${response.request}");
          throw new RPCException.expectedJSON(response);
        }

        if (response is http.StreamedResponse) {
          return http.Response.fromStream(response).then((r) => JSON.decode(r.body));
        }

        return JSON.decode(response.body);
      });

  /**
   * Handle a response which is expected to return a NO_CONTENT status.
   */
  Future _handleEmptyResponse(_RemoteProcedureCall rpc, http.BaseResponse response) =>
      _handleResponse(rpc, response)
      .then((response) {
        if (response.statusCode != HttpStatus.NO_CONTENT) {
          logger.severe("Expected empty response from ${response.request}");
          throw new RPCException.expectedEmpty(response);
        }
        return null;
      });
}




/**
 * Represents an exception which occurred while executing
 * a remote procedure call.
 */
class RPCException implements Exception {
  final http.BaseResponse response;
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

  RPCException.noRangeHeader(response):
    this(response, "Expected range header in response");

  RPCException.noContentLengthHeader(response):
    this(response, "Expected 'content-length' header in response");

  RPCException.noLocationHeader(response):
    this(response, 'Expected \'location\' header in response');

  String toString() =>
      "Request to remote procedure call $method failed with status ${response.statusCode}\n"
      "endpoint: $url\n"
      "message: ${message}";
}

class ObjectTransferException implements Exception {
  final String msg;
  ObjectTransferException(this.msg);

  toString() => msg;
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

  //TODO: Projection should be an enum.

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
 * A model of a (retryable) remote procedure call to
 * a service endpoint
 */
class _RemoteProcedureCall {
  final Uri url;

  final String method;
  final Map<String,String> headers;
  String body = null;

  _RemoteProcedureCall(
      Uri this.url,
      String this.method,
      { this.headers: const {},
        body
      }) {
    var contentType = headers[HttpHeaders.CONTENT_TYPE];
    if (contentType != null && contentType.startsWith('application/json')) {
      this.body = JSON.encode(body);
    }
  }

  _RemoteProcedureCall.fromJson(Map<String,dynamic> json):
    url = Uri.parse(json['url']),
    method = json['method'],
    headers = json['headers'],
    body = json['body'];

  http.Request asRequest() {
    http.Request request = new http.Request(method, url);

    if (body != null)
      request.body = body;

    request.headers.addAll(headers);

    return request;
  }

  Map<String,dynamic> toJson() =>
      { 'url': url.toString(),
        'method': method,
        'headers': headers,
        'body': body
      };
}


// FIXME: Workaround for quiver bug #125
// forEachAsync doesn't complete if iterable is empty.
Future _forEachAsync(Iterable iterable, Future action(var item)) {
  if (iterable.isEmpty) return new Future.value();
  return forEachAsync(iterable, action);
}