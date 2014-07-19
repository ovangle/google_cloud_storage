/**
 * Provides an API which provides filesystem like access to objects
 * within a specific bucket.
 *
 * Compatible with both the client and server versions of the library,
 * provides virtual folders and files which can be interacted with in
 * a manner which loosely resembles the `'dart:io'` filesystem API.
 */
library fs;

import 'dart:async';

import 'package:quiver/async.dart';

import 'api/api.dart';
import 'connection/connection.dart';
import 'connection/rpc.dart';
import 'source/source_common.dart';
import 'utils/content_range.dart';
import 'utils/http_utils.dart';

part 'fs/entry.dart';

const _FS_DELIMITER = "/";

/**
 * A [CloudFilesystem] is a virtual filesystem which is overlayed over
 * the contents of a particular bucket which exists on the cloud storage
 * servers.
 *
 * There are two types of entity on the filesystem:
 * - [RemoteFolder]s, which represent virtual folders within the bucket.
 * Each [RemoteFolder] (except '/') is assigned a 0-byte object with
 * mime type `text/plain` on the cloud servers when created.
 * - [RemoteFile]s, which are the file type objects stored within the filesystem.
 *
 * Each [Entry] is assigned a [:POSIX:] style path which specifies
 * how to resolve the object from the root of the filesystem. All paths are
 * specified as absolute, '/' delimited [String]s and a valid path must
 * - Not contain a whitespace character
 * - Not contain an empty component.
 */
class CloudFilesystem {
  /**
   * A connection to the cloud storage which holds metadata
   * about the
   */
  final Connection connection;
  /**
   * The bucket at the root of the current filesystem
   */
  final String bucket;

  /**
   * The folder at the root of the filesystem
   */
  RemoteFolder get root => new RemoteFolder(this, '/');

  CloudFilesystem(this.connection, String this.bucket);

  /**
   * Get the metadata associated with the bucket at the root of this
   * filesystem
   */
  Future<StorageBucket> metadata() =>
      connection.getBucket(bucket);

  /**
   * Test whether the filesystem exists on storage
   */
  Future<bool> exists() =>
      connection.getBucket(bucket, params: {'fields': 'name'})
        .then((_) => true)
        .catchError(
            (err) => false,
            test: (err) => err is RpcException && err.statusCode == 404
        );

  /**
   * Create the filesystem in storage.
   * Completes with a [FilesystemError] if the bucket at the root of
   * the filesystem already exists.
   */
  Future<CloudFilesystem> create() {
    return exists().then((exists) {
      if (exists) {
        throw new FilesystemError.rootExists(bucket);
      }
      return connection.createBucket(bucket, params: {'fields': 'name'});
    }).then((_) => this);
  }

  /**
   * Delete the filesystem from storage
   */
  Future delete() =>
      connection.deleteBucket(bucket);
}

class PathError extends Error {
  String msg;
  String path;

  PathError(this.msg, this.path);

  PathError.invalidFolder(String path):
    this("Folder path must match ${RemoteFolder.FOLDER_PATH.pattern}", path);

  PathError.invalidFile(String path):
    this("File path must match ${RemoteFile.FILE_PATH.pattern}", path);

  String toString() => "Invalid path ($path): $msg";

}

class FilesystemError extends Error {
  int errCode;
  final String message;

  FilesystemError(this.errCode, this.message);

  FilesystemError.rootExists(String bucket):
    this(4, "Bucket already exists");

  FilesystemError.noRootExists(String bucket):
    this(0, "Bucket at root of filesystem ($bucket) does not exist on the remote storage");

  FilesystemError.noSuchFolderOrFile(String name):
    this(1, "No such folder or file ($name)");

  FilesystemError.folderNotEmpty(String name):
    this(2, "Not empty");

  FilesystemError.destinationExists(String path):
    this(3, "Copy/move destination exists");

  String toString() => "Filesystem error ($errCode): $message";


}