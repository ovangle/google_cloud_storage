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

import '../matcher/matcher.dart';

import '../api/api.dart';
import '../source/source_common.dart';
import '../utils/content_range.dart';
import '../utils/either.dart';
import '../utils/http_utils.dart';
import '../json/path.dart';
import '../json/selector.dart';
import 'rpc.dart';
import 'resume_token.dart';

part 'src/bucket_requests.dart';
part 'src/object_requests.dart';
part 'src/object_transfer_requests.dart';

const API_SCOPES =
    const { PermissionRole.READER : 'https://www.googleapis.com/auth/devstorage.read_only',
            PermissionRole.WRITER : 'https://www.googleapis.com/auth/devstorage.read_write',
            PermissionRole.OWNER: 'https://www.googleapis.com/auth/devstorage.full_control'
          };

const _LIST_EQ = const ListEquality();

const _JSON_CONTENT = 'application/json; charset=UTF-8';
const _MULTIPART_CONTENT = 'multipart/related; boundary="content_boundary"';

_idHandler(RpcResponse response) => new Future.value(response);

typedef Future<dynamic> _ResponseHandler(RpcResponse response);

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

const _urlEncode = Uri.encodeComponent;

/**
 * A [Connection] with all the appropriate
 * request mixin classes.
 */

class Connection extends ConnectionBase with
    BucketRequests,
    ObjectRequests,
    ObjectTransferRequests {
  Connection(String projectId, RpcClient client):
    super(projectId, client);
}


/**
 * An implementation of the basic functionality required
 * to make a remote procedure call against google cloud storage.
 */
abstract class ConnectionBase {

  /**
   * The google assigned ID of the project which manages the objects mutated
   * by this connection.
   */
  final String projectId;

  /**
   * Send an authorised request to the server, signed with the credentials
   * used to instantiate the connection.
   */
  final RpcClient _client;

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

  ConnectionBase(this.projectId, RpcClient this._client);

  /**
   * Submit a remote procedure call to the specified [:path:] with the
   * specified [:query:] parameters.
   */
  Future<RpcResponse> _remoteProcedureCall(
      String path,
      { String method: "GET",
        Map<String,String> headers,
        Map<String,String> query,
        dynamic body
      }) {
    var rpc = new RpcRequest(path, method: method, query: query)
        ..headers.addAll(headers)
        ..jsonBody = body;

    return _client.send(rpc);
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
      { Map<String,String> headers,
        Map<String,String> query
      }) {
    assert(query['fields'] != null);
    var s = Selector.parse(query['fields']);
    if (!s.isPathInSelection(new FieldPath("nextPageToken")))
      throw new ArgumentError("'nextPageToken' must be in selected fields");

    var pageController = new StreamController<Map<String,dynamic>>();

    Future addNextPage(String nextPageToken) {
      var pageQuery = new Map.from(query)
          ..['pageToken'] = nextPageToken;
      logger.info("Retrieving paged results results for $nextPageToken");
      return _remoteProcedureCall(path, headers: headers, query: pageQuery)
          .then((rpcResponse) {
            if (!pageController.isClosed) {

              pageController.add(rpcResponse.jsonBody);

              var pageToken = rpcResponse.jsonBody['nextPageToken'];
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
      Map<String,String> query,
      Map<String,String> headers,
      void modify(dynamic obj),
      { dynamic readHandler(RpcResponse response) }) {
    return _remoteProcedureCall(path, method: "GET", query: query,
        headers: headers
    ).then((response) => readHandler(response)).then(
        (result) {
         //Apply modify on the read result.
          modify(result);
          return result;
        }
    ).then((result) {
      return _remoteProcedureCall(path, method: "GET", query: query,
          headers: headers,
          body: result
      );
    });
  }
}

class ObjectTransferException implements Exception {
  final String msg;
  ObjectTransferException(this.msg);

  toString() => msg;
}

class InvalidParameterException implements Exception {
  final String message;
  InvalidParameterException(this.message);

  toString() => "Invalid query parameter: $message";
}

// FIXME: Workaround for quiver bug #125
// forEachAsync doesn't complete if iterable is empty.
Future _forEachAsync(Iterable iterable, Future action(var item)) {
  if (iterable.isEmpty) return new Future.value();
  return forEachAsync(iterable, action);
}