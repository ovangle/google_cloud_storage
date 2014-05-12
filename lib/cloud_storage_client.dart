/**
 * Import this library to use the google cloud storage library
 * from applications which import 'dart:html'
 */
library file_storage_html;

import 'dart:async';
import 'package:http/http.dart' as http;
import 'package:google_oauth2_client/google_oauth2_browser.dart' as oauth2;

import 'connection/connection.dart';

export 'api/api.dart';
export 'source/source_client.dart';

class CloudStorageConnection extends Connection {


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
    sendAuthorisedRequest(http.BaseRequest request) {
      return context.login().then((token) {
        request.headers.addAll(oauth2.getAuthorizationHeaders(token.type, token.data));
        return request.send();
      });
    };
    return new CloudStorageConnection._(projectId, sendAuthorisedRequest);
  }

  CloudStorageConnection._(String projectId, Future<http.BaseResponse> sendAuthorisedRequest(http.BaseRequest request)) :
      super(projectId, sendAuthorisedRequest);

}

