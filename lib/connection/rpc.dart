library google_cloud_storage.connection.rpc;

import 'dart:async';
import 'dart:convert' show JSON, UTF8;
import 'dart:math' as math;
import 'dart:mirrors' as mirrors;

import 'package:http/http.dart' as http;
import 'package:logging/logging.dart';

import '../source/source_common.dart';
import '../utils/either.dart';

final _random = new math.Random();

const BASE_URL = 'www.googleapis.com';
const API_VERSION = 'v1';

Logger logger = new Logger('cloud_storage.connection');

abstract class RpcClient {
  final http.BaseClient _client;


  final String baseUrl;
  final String apiVersion;

  /**
   * The maximum number of retries per request. Each retry is tried with
   * exponential backoff, so
   * Default is `5` retries.
   */
  int maxRetryRequests = 5;
  /**
   * A set of statuses which will trigger a request retry.
   */
  final Set<int> retryStatus = new Set<int>.from([500, 502, 503, 504]);


  RpcClient(
      http.BaseClient this._client,
      { String this.baseUrl:'www.googleapis.com',
        String this.apiVersion:'v1'
      });


  /**
   * Add authorization headers to request
   */
  Future<RpcRequest> authorize(RpcRequest request);


  Future<RpcResponse> send(BaseRpcRequest rpcRequest, {bool retryRequest: true}) => _send(rpcRequest, retryRequest);

  Future<RpcResponse> _send(BaseRpcRequest request, bool retryRequest, [int retryCount=0]) =>
     authorize(request)
        .then((request) => _client.send(request.asRequest(baseUrl: baseUrl, apiVersion: apiVersion)))
        .then(http.Response.fromStream)
        .then((httpResponse) => _handleResponse(request, httpResponse, retryRequest, retryCount=retryCount));

  Future<RpcResponse> _handleResponse(BaseRpcRequest request, http.Response response, bool retryRequest, [int retryCount=0]) {
    return new Future.sync(() {

      if (retryRequest &&
          retryStatus.contains(response.statusCode) &&
          retryCount < maxRetryRequests) {
        logger.warning(
            "Remote procedure call to ${response.request.method} ${response.request.url} "
             "failed with status ${response.statusCode}\n"
             "Retrying... (retry count: $retryCount of $maxRetryRequests)"
        );
        var delay = new Duration(seconds: math.pow(2, retryCount), milliseconds: _random.nextInt(1000));
        return new Future.delayed(delay, () => _send(request, retryRequest, retryCount++));
      }

      response = new RpcResponse(response);

      return response;
    });
  }
}

abstract class BaseRpcRequest {
  /**
   * The endpoint of the request.
   * If a lvalue, the endpoint is a path from the service version url to the specific
   * rpc.
   */
  final Either<String,Uri> endpoint;
  /**
   * HTTP 1.1 method to send the request.
   */
  final String method;
  /**
   * Http Headers for the request
   */
  final Map<String,String> headers;

  /**
   * Map of query parameters for the request.
   * Ignored if the request [:endpoint:] is an rvalue.
   * Query parameter values will be automatically url encoded.
   */
  final Map<String,String> query;

  /**
   * Test whether this rpc is against an `upload` endpoint.
   * If `true`, the service name is `upload/storage`, otherwise `storage`
   */
  final bool isUploadRequest;

  BaseRpcRequest(
      dynamic /* String | Uri */ endpoint,
      { String this.method: "GET",
        Map<String,String> this.query: const {},
        bool this.isUploadRequest: false,
        Map<String, String> headers
      }):
    this.endpoint = new Either.branch(endpoint, (v) => v is Uri), this.headers = (headers != null ? headers : {}) {
  }

  /**
   * Get the request url from the method.
   * If the [:endpoint:] is an rvalue, return the value
   * If the [:endpoint:] is an lvalue, construct a new url from the lvalue, query and parameter.
   */
  Uri requestUrl({String baseUrl: BASE_URL,String apiVersion: API_VERSION}) =>
      endpoint.fold(
          ifLeft: (path) {
            var service = isUploadRequest ? 'upload/storage' : 'storage';
            return new Uri(
                scheme: 'https',
                host: baseUrl,
                path: "/$service/$apiVersion$path",
                queryParameters: query);
          },
          ifRight: (url) => url
      );


  /**
   * Returns the [RpcRequest] as a [http.Request], for sending to server.
   */
  http.Request asRequest();
}

class RpcRequest extends BaseRpcRequest {

  String body;

  /**
   * The body of the request as a JSON map
   */
  Map<String,dynamic> get jsonBody {
    if (!headers['content-type'].startsWith('application/json')) {
      throw new StateError("Not an 'application/json' request");
    }
    if (body != null) {
      return JSON.decode(body);
    }
    return null;
  }

  /**
   * Set the body of the request. The argument must be a json encodable object
   */
  set jsonBody(dynamic body) {
    this.body = JSON.encode(body);
  }

  RpcRequest(
      dynamic /* String | Uri */ endpoint,
      { String method: "GET",
        Map<String,String> query: const {},
        bool isUploadRequest: false,
        Map<String, String> headers
      }):
    super(endpoint, method: method, query: query, isUploadRequest: isUploadRequest, headers: headers);

  factory RpcRequest.fromJson(Map<String,dynamic> json) {

    if (json['endpoint'] == null || json['endpoint'] is! Map)
      throw new ArgumentError("No 'endpoint' in json");
    var endpoint = json['endpoint'];

    var lvalue = endpoint['left'], rvalue=endpoint['right'];

    if (lvalue == null && rvalue == null)
      throw new ArgumentError("Invalid 'endpoint' in json");
    if (lvalue != null && rvalue != null)
      throw new ArgumentError("Invalid 'endpoint' in json");
    if (lvalue != null) {
      endpoint = lvalue;
    }
    if (rvalue != null) {
      endpoint = Uri.parse(rvalue);
    }


    json.putIfAbsent('method', () => 'GET');
    json.putIfAbsent('query', () => {});
    json.putIfAbsent('isUploadRequest', () => false);
    var req = new RpcRequest(
        endpoint,
        method: json['method'],
        query: json['query'],
        isUploadRequest: json['isUploadRequest']
    );
    if (json['headers'] is Map) {
      json['headers'].forEach((k,v) => req.headers[k] = v);
    }
    if (json['body'] is String) {
      req.body = json['body'];
    }
    return req;
  }

  @override
  http.Request asRequest({String baseUrl: BASE_URL, String apiVersion: API_VERSION}) {
    var req = new http.Request(method, requestUrl(baseUrl: baseUrl, apiVersion: apiVersion))
        ..headers.addAll(headers);

    if (body != null) {
      req.body = body;
    }

    return req;
  }

  toJson() =>
      { 'endpoint': endpoint.toJson(),
        'method': method,
        'query': query,
        'headers': headers,
        'body': body,
        'isUploadRequest': isUploadRequest
      };
}

class StreamedRpcRequest extends BaseRpcRequest {

  /**
   * When adding a [Source] to the request, load bytes
   * from the [Source] in chunks of [_BUFFER_SIZE].
   */
  static const int _BUFFER_SIZE = 5 * 1024 * 1024;

  StreamController _controller = new StreamController();


  StreamedRpcRequest(
      dynamic /* String | Uri */ endpoint,
      { String method: "GET",
        Map<String,String> query: const {},
        bool isUploadRequest: false }):
    super(endpoint, method: method, query: query, isUploadRequest: isUploadRequest),
    this._controller = new StreamController<List<int>>();

  @override
  http.BaseRequest asRequest({String baseUrl: BASE_URL, String apiVersion: API_VERSION}) {
    var req = new http.StreamedRequest(method, requestUrl(baseUrl: baseUrl, apiVersion: apiVersion));
    req.headers.addAll(headers);
    //Pipe all bytes from the stream sink into the request.
    _controller.stream.pipe(req.sink as StreamSink);
    return req;
  }

  /**
   * A [StreamSink] to insert request headers
   */
  EventSink<List<int>> get sink => _controller.sink;

  /**
   * Add bytes from [Source] to the request, beginning at [:start:].
   */
  Future addSource(Source source, [int start=0]) {
    return new Future.sync(() {
      if (start >= source.length)
        return sink.close();

      source.setPosition(start);
      var readLen = math.min(start + _BUFFER_SIZE, source.length);
      return source.read(readLen).then((bytes) {
        sink.add(bytes);
        return addSource(source, start + _BUFFER_SIZE);
      });
    });
  }
}

class RpcResponse implements http.Response {
  final mirrors.InstanceMirror _response;

  Map<String,dynamic> get jsonBody {
    if (!headers.containsKey('content-type'))
      throw new RpcException.expectedResponseHeader('content-type', this);
    if (!headers['content-type'].startsWith('application/json'))
      throw new RpcException.expectedJson(this);
    return JSON.decode(body);
  }

  RpcResponse(http.Response _response):
    this._response = mirrors.reflect(_response);


  dynamic noSuchMethod(Invocation invocation) {
    return _response.delegate(invocation);
  }

}


class RpcException {
  final http.Response response;
  final String message;

  RpcException(this.message, this.response);

  RpcException.invalidStatus(http.Response response):
    this("Invalid status: ${response.statusCode}", response);

  RpcException.expectedJson(http.Response response):
    this("Expected JSON response", response);

  RpcException.expectedResponseHeader(String header, http.Response response):
    this("Expected '$header' header in response", response);

  toString() => "RPC Exception: $message\n"
                "${response.body}";
}


