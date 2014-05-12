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
import 'source/source_common.dart';
import 'utils/content_range.dart';
import 'utils/http_utils.dart';

part 'fs/src/entry.dart';

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

  CloudFilesystem(this.connection, String this.bucket);

  //A cached version of the bucket at the root of the filesystem.
  StorageBucket _cachedBucket;

  Future<StorageBucket> metadata() =>
      connection.getBucket(bucket);
}

class PathError extends Error {
  String msg;
  String path;

  PathError(this.msg, this.path);

  PathError.invalidPath(String path):
    this("Path must be specified as $_FS_DELIMITER delimited list of non-empty components.\n"
          "and cannot contain a whitespace character", path);

  PathError.invalidFolder(String path):
    this("Folder name must end with $_FS_DELIMITER", path);

  PathError.invalidFile(String path):
    this("File name cannot end with $_FS_DELIMITER", path);

  String toString() => "$msg: $path";

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

  FilesystemError.destinationExists(String path):
    this(3, "Copy/move destination exists");

  String toString() => "Filesystem error ($errCode): $message";


}