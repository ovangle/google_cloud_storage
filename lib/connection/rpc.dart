library google_cloud_storage.connection.rpc;

import 'dart:async';
import 'dart:convert' show JSON, UTF8;
import 'dart:math' as math;
import 'dart:mirrors' as mirrors;
import 'dart:typed_data';

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

  Future<http.StreamedResponse> sendHttp(BaseRpcRequest request) =>
    authorize(request)
        .then((request) => _client.send(request.asRequest(baseUrl: baseUrl, apiVersion: apiVersion)));

  Future<RpcResponse> send(BaseRpcRequest rpcRequest, {bool retryRequest: true}) => _send(rpcRequest, retryRequest);

  Future<RpcResponse> _send(BaseRpcRequest request, bool retryRequest, [int retryCount=0]) =>
     authorize(request)
        .then((request) => _client.send(request.asRequest(baseUrl: baseUrl, apiVersion: apiVersion)))
        .then((response) => http.Response.fromStream(response))
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
    this.endpoint = new Either.branch(endpoint, (v) => v is Uri),
    this.headers = (headers != null ? headers : {});

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
    if (body != null) this.body = JSON.encode(body);
  }

  RpcRequest(
      dynamic /* String | Uri */ endpoint,
      { String method: "GET",
        Map<String,String> query: const {},
        bool isUploadRequest: false,
        Map<String, String> headers
      }):
    super(endpoint, method: method, query: query, isUploadRequest: isUploadRequest, headers: headers);

  @override
  http.Request asRequest({String baseUrl: BASE_URL, String apiVersion: API_VERSION}) {
    var req = new http.Request(method, requestUrl(baseUrl: baseUrl, apiVersion: apiVersion))
        ..headers.addAll(headers);

    if (body != null) {
      req.body = body;
    }

    return req;
  }
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
    print(req.headers);
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
    return new Future.value().then((_) {
      if (start >= source.length)
        return sink.close();

      source.setPosition(start);
      var readLen = math.min(start + _BUFFER_SIZE, source.length) - start;
      return source.read(readLen).then((bytes) {
        sink.add(bytes);
        return addSource(source, start + _BUFFER_SIZE);
      });
    });
  }
}

class MultipartRelatedRpcRequest extends BaseRpcRequest {
  static const String _BOUNDARY = 'multipart_boundary';

  MultipartRelatedRpcRequest(
      dynamic /* String | Uri */ endpoint,
      { String method: "GET",
        Map<String,String> query: const {},
        bool isUploadRequest: false,
        Map<String, String> headers
      }):
      super(
          endpoint,
          method: method,
          isUploadRequest: isUploadRequest,
          headers: headers
      );

  List <RpcRequestPart> requestParts = [];


  List<int> get _bodyBytes {
    List<int> bytes = new List<int>();
    for (var part in requestParts) {
      bytes
          ..addAll(UTF8.encode('--multipart_boundary\r\n'))
          ..addAll(UTF8.encode('content-type: ${part.contentType}\r\n'))
          ..addAll(UTF8.encode('\r\n'))
          ..addAll(part.bodyBytes)
          ..addAll(UTF8.encode('\r\n'));
    }
    bytes..addAll(UTF8.encode('--$_BOUNDARY--\r\n'));
    return bytes;
  }


  @override
  http.Request asRequest({String baseUrl: BASE_URL, String apiVersion: API_VERSION}) {
    headers['content-type'] = 'multipart/related; boundary="$_BOUNDARY"';
    var req = new http.Request(method, requestUrl(baseUrl: baseUrl, apiVersion: apiVersion));
    req.headers.addAll(headers);
    req.bodyBytes = _bodyBytes;
    return req;
  }
}

/**
 * Part of a [MultipartRelatedRpcRequest]
 */
class RpcRequestPart {
  final String contentType;
  List<int> bodyBytes;

  RpcRequestPart(this.contentType);

  String get body => UTF8.decode(bodyBytes);
  set body(String value) {
    bodyBytes = UTF8.encode(value);
  }

  Map<String,dynamic> get jsonBody => JSON.decode(body);
  set jsonBody(dynamic value) {
    body = JSON.encode(value);
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

  int get statusCode => response.statusCode;

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


