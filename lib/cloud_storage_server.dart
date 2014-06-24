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
import 'connection/rpc.dart';

import "package:json_web_token/json_web_token.dart";

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
   *   to the cloud storage account. Defaults to `READER` access.
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
          role = PermissionRole.READER;
        scopes = [ 'https://www.googleapis.com/auth/userinfo.email',
                   API_SCOPES[role]
                 ].join(" ");
      }
      var console = new oauth2.ComputeOAuth2Console(
          projectNumber,
          iss: serviceAccount,
          privateKey: privateKey,
          scopes: scopes);


      return new CloudStorageConnection._(projectId, new _IOClient(console));
    });
  }

  CloudStorageConnection._(String projectId, _IOClient client):
    super(projectId, client);
}

class _IOClient extends RpcClient {
  final oauth2.ComputeOAuth2Console console;

  _IOClient(this.console):
    super(new http.Client());


  @override
  Future<BaseRpcRequest> authorize(BaseRpcRequest request) {
    //FIXME: This is wrong when running on compute engine.
    var client = new oauth2.OtherPlatformClient(console.projectId, console.privateKey, console.iss, console.scopes);
    return new Future.value().then((_) {
      return JWTStore.getCurrent().generateJWT(client.iss, client.scopes);
    }).then((JWT jwt) {
      return request
          ..headers['authorization'] = 'Bearer ${jwt.accessToken}';
    });
  }
}

