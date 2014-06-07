/**
 * Import this library to use the google cloud storage library
 * from applications which import 'dart:html'
 */
library file_storage_html;

import 'dart:async';
import 'package:http/http.dart' as http;
import 'package:http/browser_client.dart' as client_http;
import 'package:google_oauth2_client/google_oauth2_browser.dart' as oauth2;

import 'connection/connection.dart' as base;

export 'api/api.dart';
export 'source/source_client.dart';
export 'connection/connection.dart' show RPCException;

class CloudStorageConnection extends base.Connection {

  /**
   * Create a new connection to the google cloud storage library with the
   * given [:client:].
   *
   * [:projectId:] is the google assigned identifier of the owner of the
   * buckets to connect to.
   * [GoogleOAuth2] is an oauth2 context which can authenticate the client
   * with the cloud storage API.
   */
  factory CloudStorageConnection(
      String projectId,
      oauth2.GoogleOAuth2 context) {
    return new CloudStorageConnection._(
        projectId,
        new _CloudStorageClient(context)
    );
  }

  CloudStorageConnection._(String projectId, http.Client client) :
      super(projectId, client);

}

class _CloudStorageClient extends client_http.BrowserClient {
  oauth2.GoogleOAuth2 context;

  _CloudStorageClient(this.context) : super();

  Future<http.StreamedResponse> send(http.BaseRequest request) {
    return context.login().then((token) {
      request.headers.addAll(oauth2.getAuthorizationHeaders(token.type, token.data));
      return super.send(request);
    });
  }

}

