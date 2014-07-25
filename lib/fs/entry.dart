part of fs;

/**
 * A file or folder which exists on the google cloud servers.
 */
abstract class RemoteEntry {

  final Filesystem filesystem;
  final String path;

  /**
   * Create a new entry from the filesystem and path.
   * If [:path:] ends with '/', a folder will be created
   * otherwise a file
   */
  factory RemoteEntry(Filesystem filesystem, String path) {
    return (RemoteFolder.FOLDER_PATH.matchAsPrefix(path) != null)
        ? new RemoteFolder(filesystem, path)
        : new RemoteFile(filesystem, path);
  }

  factory RemoteEntry.fromParentAndName(RemoteFolder parent, String name) {
    return new RemoteEntry(parent.filesystem, parent.path + name);
  }

  RemoteEntry._(this.filesystem, this.path);

  /**
   * The folder that contains the current entry.
   */
  RemoteFolder get parent;

  /**
   * The name of the metadata object
   */
  String get _objectName => path.substring(1);

  /**
   * The name of the entry within the parent folder
   */
  String get name => path.substring(parent.path.length);

  Future<bool> exists() {
    if (path == _FS_DELIMITER) return new Future.value(true);
    return filesystem.connection.getObject(
        filesystem.bucket,
        _objectName,
        params: {'fields':'name'})
    .then((result) => true)
    .catchError(
        (err) => false,
        test: (err) =>
            err is RpcException &&
            err.statusCode == HttpStatus.NOT_FOUND
    );
  }

  /**
   * An entry property is a string value stored in the metadata of the
   * file storage metadata.
   *
   * If `orElse` is not null and the value wasn't found in the metadata
   * of the associated entry, the value will be returned.
   */
  Future<String> getEntryProperty(String key, {orElse()}) =>
      filesystem.connection.getObject(
         filesystem.bucket,
         _objectName,
         params: {'fields': 'metadata($key)'})
      .then((object) {
        if (object.metadata != null && object.metadata.containsKey(key))
          return object.metadata[key];
        return orElse != null ? orElse() : null;
      });


  Future setEntryProperty(String key, String value) {
    return filesystem.connection.patchObject(
        filesystem.bucket,
        _objectName,
        (object) => object.metadata[key] = value,
        params: {'fields': 'metadata'}
    );
  }

  /**
   * Get the google cloud storage metadata associated with the current
   * [RemoteEntry].
   *
   * Note: The metadata of the root of the filesystem is always `null`.
   */
  Future<StorageObject> metadata() =>
      filesystem.connection.getObject(filesystem.bucket, _objectName);

  /**
   * Delete the [RemoteEntry].
   */
  Future<RemoteEntry> delete({bool recursive: false});


  bool operator ==(Object other) =>
      other is RemoteEntry &&
      other.filesystem == filesystem &&
      other.path == path;

  int get hashCode => qcore.hash2(filesystem, path);
}


/// A virtual folder in the filesystem. A folder path must match the regular
/// expression (/[^\s/]+)*/
class RemoteFolder extends RemoteEntry {
  static final RegExp FOLDER_PATH = new RegExp(r'^(/[^\s/]+)*/$');


  RemoteFolder(filesystem, String path):
    super._(filesystem, path) {
    if (FOLDER_PATH.matchAsPrefix(path) == null) {
      throw new PathError.invalidFolder(path);
    }
  }

  RemoteFolder.fromParentAndName(RemoteFolder parent, String name):
    this(parent.filesystem, parent.path + name);

  RemoteFolder.fromObject(Connection connection, StorageObject obj):
    this(
        new Filesystem(connection, obj.bucket),
        '/' + obj.name
    );

  RemoteFolder get parent {
    if (path == _FS_DELIMITER) {
      return this;
    }
    return new RemoteFolder(
        filesystem,
        path.substring(0, path.lastIndexOf(_FS_DELIMITER, path.length - 2) + 1)
    );
  }

  /**
   * List all the [RemoteEntry]s in the current folder.
   */
  Stream<RemoteEntry> list() {
    return filesystem.connection.listObjects(
        filesystem.bucket,
        params: {'prefix': _objectName, 'delimiter': _FS_DELIMITER }
    )
    .map((prefixOrObject) =>
        prefixOrObject.fold(
            ifLeft: (prefix) => new RemoteFolder(filesystem, '/' + prefix),
            ifRight: (obj) => new RemoteEntry(filesystem, '/' + obj.name)
        )
    )
    .where((entry) => entry.name != _objectName);
  }


  Future<bool> get isEmpty => list().isEmpty;

  /**
   * Create the folder *if it does not exist*.
   *
   * Returns silently if the folder already exists.
   */
  Future<RemoteFolder> create() {
    return exists().then((exists) {
      if (!exists) {
        return filesystem.connection
            .uploadObjectSimple(
                filesystem.bucket,
                _objectName,
                new ByteSource([], 'text/plain'),
                params: {'fields': 'name'}
            );
      }
    }).then((_) => this);
  }

  /**
   * Deletes the folder from the remote filesystem.
   *
   * If [:recursive:] is `false` then also delete the contents of the [RemoteFolder].
   * Otherwise, returns a future which completes with a [FilesystemError].
   */
  @override
  Future<RemoteFolder> delete({recursive: false}) {
    return isEmpty.then((result) {
      if (!result) {
        if (recursive) {
          return list().forEach((entry) => entry.delete(recursive: recursive));
        }
        throw new FilesystemError.folderNotEmpty(path);
      }
    })
    .then((_) => filesystem.connection.deleteObject(filesystem.bucket, path))
    .then((_) => this);
  }


  String toString() => "Folder: $path";
}

/**
 * A file in the filesystem. File paths must match the pattern `(/[^\s/]+)+`
 */
class RemoteFile extends RemoteEntry {
  static final RegExp FILE_PATH = new RegExp(r'(/[^\s/]+)+$');

  RemoteFile(filesystem, path):
      super._(filesystem, path)  {
    if (FILE_PATH.matchAsPrefix(path) == null)
      throw new PathError.invalidFile(path);
  }

  RemoteFile.fromParentAndName(RemoteFolder parent, String name):
    this(parent.filesystem, parent.path + name);

  RemoteFolder get parent =>
      new RemoteFolder(
          filesystem,
          path.substring(0, path.lastIndexOf(_FS_DELIMITER) + 1)
      );


  /**
   * writes the [RemoteFile] to the server, overwriting any existing
   * content of the file.
   *
   * The file content is read from the given [Source].
   * [:contentType:] is the mime type of the source.
   */
  Future<RemoteFile> write(Source source) {
    return filesystem.connection.uploadObject(
        filesystem.bucket,
        _objectName,
        source,
        params: {'fields': 'name'}
    ).then((resumeToken) => this);
  }

  /**
   * Reads the content of the file
   */
  Stream<List<int>> read([int start_or_end, int end]) {
    Range range = null;
    if (start_or_end != null) {
      if (start_or_end < 0) throw new RangeError.value(start_or_end);
      if (end != null) {
        if (end <= start_or_end) throw new RangeError.value(end);
        //A [Range] includes the index of the end byte.
        range = new Range(start_or_end, end - 1);
      } else {
        range = new Range(0, start_or_end - 1);
      }
    }
    return filesystem.connection.downloadObject(
        filesystem.bucket,
        _objectName,
        range: range);
  }

  /**
   * Create a copy of the [RemoteFile] to the specified filesystem location.
   * The destination does not necessarily need to be in the same fileysystem.
   *
   * Throws a [FilesystemError] if there is already an object at the specified
   * location.
   *
   * Returns a [Future] which completes with the created file if none exists,
   * or completes with a [FilesystemError] if the destination object exists.
   */
  Future<RemoteFile> copyTo(RemoteFile file) {
    return file.exists().then((result) {
      if (result) throw new FilesystemError.destinationExists(path);
      return filesystem.connection.copyObject(
          filesystem.bucket,
          _objectName,
          file.filesystem.bucket,
          file._objectName,
          params: {'fields': 'name'});
      })
      .then((obj) => new RemoteFile(file.filesystem, obj.name));
  }

  /**
   * Move the [RemoteFile] to the specified fileysystem location.
   * The destination does not necessarily need to be in the same [Filesystem]
   * as the current file.
   *
   * Returns a [Future] which completes with the created file if none exists,
   * or completes with a [FilesystemError] if the destination object exists.
   */
  Future<RemoteFile> moveTo(RemoteFile file) {
    return copyTo(file)
        .then((file) {
          return this.delete()
              .then((_) => file);
        });
  }

  /**
   * Delete the object from the filesystem.
   */
  Future<RemoteFile> delete({bool recursive: false}) =>
      filesystem.connection.deleteObject(
          filesystem.bucket,
          _objectName)
      .then((_) => this);

  Future<int> get length =>
      filesystem.connection.getObject(filesystem.bucket, path, params: {'fields': 'size'})
      .then((result) => result.size);

  String toString() => "RemoteFile: $path";
}
