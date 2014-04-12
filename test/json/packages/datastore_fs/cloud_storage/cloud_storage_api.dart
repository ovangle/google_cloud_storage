library google_cloud_storage.api;

import 'dart:async';
import 'dart:convert' show JSON;
import 'dart:io';
import 'package:http/http.dart' as http;

import 'package:google_oauth2_client/google_oauth2_console.dart' as oauth2;

import '../json/object.dart';

part 'src/access_controls.dart';
part 'src/entry.dart';
part 'src/misc.dart';


const API_VERSION = 'v1beta2';

const API_ENDPOINT = 'https://storage.google.com';

const API_SCOPES = 
    const { PermissionRole.READER : 'https://googleapis.com/auth/devstorage.read_only',
            PermissionRole.WRITER : 'https://googleapis.com/auth/devstorage.read_write',
            PermissionRole.OWNER: 'https://googleapis.com/auth/devstorage.full_control'
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
        scopes = [API_SCOPES[role]];
      }
      oauth2.ComputeOAuth2Console console = new oauth2.ComputeOAuth2Console(
          projectNumber,
          iss: serviceAccount,
          privateKey: privateKey,
          scopes: scopes);
      _sendAuthorisedRequest(http.Request request) =>
          console.withClient((client) => client.send(request));
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
  String platformUrl(String path, 
                     { Map<String,String> query: const {}, 
                       String apiBaseUrl: API_ENDPOINT,
                       String apiVersion: API_VERSION
                     }) {
    query['project'] = projectId;
    var q = query.keys.map((k) => "$k=${query[k]}").join("&");
    return "$apiBaseUrl/storage/${apiVersion}${path}?${q}";
  }
  
  /**
   * Make the uri request via [:method:] via [:url:]
   * with the [:body:] and [:headers:].
   * Implementation differs between `console` and `html` clients.
   */
  Future<http.Response> send(
      String method, String url, 
      String body, Map<String,String> headers) {
    Uri requestUri = Uri.parse(url);
    http.Request request = new http.Request(method, requestUri)
        ..headers.addAll(headers)
        ..body = body;
    return _sendAuthorisedRequest(request)
        .then((http.StreamedResponse response) => http.Response.fromStream(response));
  }
  
  /**
   * Make the url request to the given api [:path:] via the specified [:method:].
   * 
   * Throws a `RPCException` if the status code is not `200 OK`.
   * If [:expectJson:] is 
   */
  Future</* String | Map<String,dynamic> */dynamic> request(String method, String path,
      { Map<String,String> query: const {}, 
        /* Map or String */ var body, 
        String contentType,
        String apiBaseUrl: API_ENDPOINT,
        String apiVersion: API_VERSION,
        bool expectJson: true}) {
    if (body is Map) {
      body = JSON.encode(body);
      contentType = 'application/json';
    }
    var headers = { 'content-type': contentType };
    var url = platformUrl(path, query: query, apiBaseUrl: apiBaseUrl, apiVersion: apiVersion);
    return send(method, url, body, headers)
        .then((response) {
          if (response.statusCode < 200 || response.statusCode >= 300)
            throw new RPCException(response, method, url);
          if (expectJson) {
            if (response.headers['content-type'] != 'application/json')
              throw "Unexpected content type in response: ${response.headers['content-type']}";
            return JSON.decode(response.body);
          }
          return response.body;
        });
  }
  
  /**
   * Get the bucket with the given name from the datastore. 
   */
  Future<Bucket> getBucket(String name, {String selector: "*"}) {
    
  }
}


class RPCException implements Exception {
  final http.Response response;
  final String method;
  final String url;
  
  RPCException(this.response, this.method, this.url);
  
  String toString() => 
      "Request to remote procedure call $method failed with status ${response.statusCode}\n"
      "endpoint: $url";
}
  

