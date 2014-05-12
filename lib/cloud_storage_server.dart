/**
 * Import this library to use the cloud storage library from
 * applications which import 'dart:io'.
 *
 * Requires access to a service account authorised to connect to
 * a remote instance of the cloud storage library or to be run from
 * a compute engine instance
 */
library file_storage;

import 'dart:async';
import 'dart:io';
import 'package:google_oauth2_client/google_oauth2_console.dart' as oauth2;
import 'package:http/http.dart' as http;

import 'api/api.dart';
import 'connection/connection.dart';

export 'api/api.dart';
export 'connection/connection.dart';
export 'source/source_server.dart';

class CloudStorageConnection extends Connection {

  /**
   * Open a new connection to the cloud storage server.
   * [:projectNumber:] is a google assigned id associated with the project.
   * [:projectId:] is a google assigned identifier for the project.
   *
   * If connecting via a service account, the following must be also be provided
   * - A [PermissionRole] which indicates the level of access the service account has been granted
   *   to the cloud storage account
   * - The email of the service account
   * - A file system path to a private key file in `.pem` format which can be used
   *   to authenticate the service account with the datastore.
   *
   * If no service account is provided, the level of authentication assumed by the
   * connection is read from the scopes provided at creation of the compute engine
   * VM instance.
   */
  static Future<CloudStorageConnection> open(String projectNumber, String projectId,
      { PermissionRole role, String serviceAccount, String pathToPrivateKey}) {

    Future<String> _readPrivateKey(String path) =>
        (path == null)
        ? new Future.value()
        : new File(path).readAsString();

    return _readPrivateKey(pathToPrivateKey).then((privateKey) {
      var scopes;
      if (serviceAccount != null && pathToPrivateKey != null) {
        if (role == null)
          throw 'A role must be provided';
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
    super(projectId, sendAuthorisedRequest);
}


