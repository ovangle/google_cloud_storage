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
import 'dart:math' as math;

import 'package:crypto/crypto.dart' show MD5;
import 'package:google_oauth2_client/google_oauth2_console.dart' as oauth2;
import 'package:http/http.dart' as http;

import 'api/api.dart';
import 'connection/connection.dart';

export 'api/api.dart';
export 'connection/connection.dart';

class CloudStorageConnection extends ConnectionBase
with BucketRequests,
     ObjectRequests,
     ObjectTransferRequests {

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


/**
 * Files are searchable and can be uploaded in a single contiguous
 * chunk.
 */
class FileSource implements Source {
  static final CHUNK_SIZE = Source.CHUNK_SIZE;

  final File _file;

  final Function onError;

  bool _started = false;
  int _fileLength;
  int _filePos;

  FileSource(this._file, {this.onError});

  int get length {
    if (_fileLength == null)
      _fileLength = _file.lengthSync();
    return _fileLength;
  }

  int get currentPosition => _filePos != null ? math.min(_filePos, _fileLength) : null;

  Future<List<int>> current() {
    return _file.open(mode: FileMode.READ).then((f) {
      return f.setPosition(_filePos)
          .then((file) => file.read(CHUNK_SIZE))
          .catchError(onError)
          .whenComplete(() => f.close());
    });
  }

  bool moveNext() {
    if (!_started) {
      _started = true;
      return true;
    }
    _filePos += CHUNK_SIZE;
    return _filePos < length;
  }

  void setPosition(int position) {
    if (position < 0 || position >= _fileLength)
      throw new RangeError.range(position, 0, _fileLength - 1);
    _filePos = position;
  }


  @override
  Future<List<int>> md5Hash() {
    var md5 = new MD5();
    var completer = new Completer();
    _file.openRead().listen(
        md5.add,
        onError: completer.completeError,
        onDone: () => completer.complete(md5.close())
    );
    return completer.future;
  }

  @override
  Future close() => new Future.value();
}
