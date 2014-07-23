/**
 * Import this library to use the google cloud storage library
 * from applications which import 'dart:html'
 */
library file_storage_html;

import 'dart:async';
import 'package:google_oauth2_client/google_oauth2_browser.dart' as oauth2;
import 'package:http/browser_client.dart' show BrowserClient;

import 'connection/connection.dart' as base;
import 'connection/rpc.dart';

export 'api/api.dart';
export 'source/source_client.dart';
export 'connection/rpc.dart' show RpcException;


/**
 * A connection to the cloud storage server.
 *
 * This method is not intended to be used directly. Instead, once a connection
 * has been established, a [Filesystem] can be created from the connection and
 * the bucket name, which provides more familiar, filesystem like access to
 * objects on storage.
 *
 * For more details and a full example, see `example/server_storage.dart`
 */
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

  CloudStorageConnection._(String projectId, RpcClient client) :
      super(projectId, client);

}

class _CloudStorageClient extends RpcClient {
  oauth2.GoogleOAuth2 context;

  _CloudStorageClient(this.context) : super(new BrowserClient());

  Future<BaseRpcRequest> authorize(BaseRpcRequest request) {
    return context.login().then((token) {
      return request
          ..headers['authorization'] = 'Bearer ${token.data}';
    });
  }

}

