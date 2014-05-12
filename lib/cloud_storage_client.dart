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


  factory CloudStorageConnection(
      String projectId,
      List<String> scopes,
      { tokenLoaded(oauth2.Token token),
        bool autoLogin: false,
        String approvalPrompt
      }) {
    oauth2.GoogleOAuth2 context = new oauth2.GoogleOAuth2(
        projectId,
        scopes,
        tokenLoaded: tokenLoaded,
        autoLogin: autoLogin,
        approval_prompt: approvalPrompt);
    sendAuthorisedRequest(http.BaseRequest request) {
      context.login().then((token) {
        request.headers.addAll(oauth2.getAuthorizationHeaders(token.type, token.data));
        return request.send();
      });
    };
    return new CloudStorageConnection._(projectId, sendAuthorisedRequest);
  }


  CloudStorageConnection._(String projectId, Future<http.BaseResponse> sendAuthorisedRequest(http.BaseRequest request)) :
      super(projectId, sendAuthorisedRequest);

}

