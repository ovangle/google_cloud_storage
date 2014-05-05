library fs;

import 'dart:async';
import 'dart:io';

import 'package:quiver/async.dart';

import '../api/api.dart';

part 'src/entry.dart';

const _FS_DELIMITER = "/";

/**
 * A [CloudFilesystem] is a filesystem that resembles
 * the API exported by `dart:io`, but where
 */
class CloudFilesystem {
  /**
   * A connection to the cloud storage which holds metadata
   * about the
   */
  final CloudStorageConnection connection;
  /**
   * The bucket at the root of the current filesystem
   */
  final String bucket;

  CloudFilesystem(this.connection, String this.bucket);

  //A cached version of the bucket at the root of the filesystem.
  StorageBucket _cachedBucket;

  Future<StorageBucket> get _bucketMetadata {
    if (_cachedBucket == null) {
      return connection.getBucket(bucket)
          .then((bucket) {
            _cachedBucket = bucket;
            return bucket;
          })
          .catchError((err) {
            if (err is RPCException && err.statusCode == HttpStatus.NOT_FOUND) {
              throw new FilesystemError.noRootExists(bucket);
            }
            throw err;
          });
    }
    return new Future.value(_cachedBucket);
  }
}

class FilesystemError extends Error {
  int errCode;
  final String message;

  FilesystemError(this.errCode, this.message);

  FilesystemError.noRootExists(String bucket):
    this(0, "Bucket at root of filesystem ($bucket) does not exist on the remote storage");

  FilesystemError.noSuchFolderOrFile(String name):
    this(1, "No such folder or file ($name)");

  FilesystemError.folderNotEmpty(String name):
    this(2, "Not empty");

  String toString() => "Filesystem error ($errCode): $message";


}