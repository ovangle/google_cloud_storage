part of fs;

abstract class Entry {
  final CloudFilesystem filesystem;
  final String name;

  Map<String,String> _cachedMetadata;

  Entry(this.filesystem, this.name):
    this._cachedMetadata = {};

  static final _FOLDER_REGEXP = new RegExp(r'/.');
  /**
   * The folder that contains the current entry.
   */
  Folder get parent =>
      new Folder(filesystem, name.substring(0, name.lastIndexOf(_FOLDER_REGEXP)));

  Future<bool> exists() {
    if (name == "/") return new Future.value(true);
    return filesystem.connection.getObject(
        filesystem.bucket,
        name,
        selector: "name")
    .then((result) => true)
    .catchError(
        (err) => false,
        test: (err) =>
            err is RPCException &&
            err.statusCode == HttpStatus.NOT_FOUND
    );
  }

  Future setEntryProperty(String key, String value) {
    return filesystem.connection.updateObject(
        filesystem.bucket,
        name,
        (object) => object.metadata[key] = value,
        readSelector: "metadata",
        resultSelector: "metadata"
    ).then((obj) => _cachedMetadata = obj.metadata);
  }

  Future<String> getEntryProperty(String key) {
    if (_cachedMetadata.containsKey(key))
      return new Future.value(_cachedMetadata[key]);
    return filesystem.connection.getObject(
        filesystem.bucket,
        name,
        selector: "metadata").then((object) {
      _cachedMetadata = object.metadata;
      return _cachedMetadata[key];
    });
  }

  void _clearCaches() {
    _cachedMetadata = {};
  }




  Future<Entry> delete({bool recursive: false});

  bool operator ==(Object other) => other is Entry && other.name == name;
  int get hashCode => name.hashCode * 7;
}

class Folder extends Entry {

  Folder(filesystem, String name):
    super(filesystem, name.endsWith(_FS_DELIMITER) ? name : name + _FS_DELIMITER);

  /**
   * List all the [Entry]s in the current folder.
   */
  Stream<Entry> list() {
    return filesystem.connection.listBucketContents(
        filesystem.bucket,
        (this.name == _FS_DELIMITER ? "" : this.name),
        delimiter: _FS_DELIMITER,
        selector: "name,contentType"
    )
    .map((prefixOrObject) =>
        prefixOrObject.fold(
            ifLeft: (prefix) => new Folder(filesystem, prefix),
            ifRight: (obj) => new RemoteFile(filesystem, obj.name, obj.contentType)
        )
    )
    .where((obj) => obj != this);
  }

  Future<bool> get isEmpty => list().isEmpty;

  Future<Folder> create({recursive: false}) {
    return exists().then((result) {
      if (result) {
        return this;
      } else {
        return parent.exists().then((result) {
          if (!result) {
            if (recursive) return parent.create(recursive: true);
            throw new FilesystemError.noSuchFolderOrFile(parent.name);
          }
        })
        .then((_) => filesystem.connection.uploadObject(filesystem.bucket, name, 'text/plain', [], selector: "name"))
        .then((obj) => new Folder(filesystem, obj.name));
      }
    });

  }

  @override
  Future<Folder> delete({recursive: false}) {
    return isEmpty.then((result) {
      if (!result) {
        if (recursive) {
          return list().toList()
              .then((entries) => forEachAsync(entries, (entry) => entry.delete));
        }
        throw new FilesystemError.folderNotEmpty(name);
      }
    })
    .then((_) => filesystem.connection.deleteObject(filesystem.bucket, name))
    .then((_) => this);
  }

  String toString() => "Folder: $name";
}

class RemoteFile extends Entry {
  /**
   * The content type of the file.
   */
  final ContentType contentType;

  var _cachedStorageObject;

  RemoteFile(filesystem, name, String contentType):
      super(filesystem, name),
      this.contentType = ContentType.parse(contentType);

  RemoteFile._from(RemoteFile file):
    super(file.filesystem, file.name),
    this.contentType = file.contentType;

  /**
   * Upload the storage object to the server. Will overwrite any existing file
   * with the same name.
   *
   * Suitable for files of size up to `5MB`. For files of size >= 5MB, the resumable
   * write function should be used instead.
   */
  Future<RemoteFile> write(List<int> bytes) {
    return filesystem.connection.uploadObject(
        filesystem.bucket,
        name,
        contentType.toString(),
        bytes,
        selector: "name,contentType"
    ).then((obj) => new RemoteFile(filesystem, obj.name, obj.contentType));
  }

  ResumableUploadSink writeResumable() {
    StreamController controller = new StreamController<List<int>>();

    ResumableUploadSink uploadSink = new ResumableUploadSink._(controller.sink);

    filesystem.connection.resumableUploadObject(
        filesystem.bucket,
        name,
        contentType.toString(),
        controller.stream
    )
    //FIXME: This is plain wrong.
    .then((result) => uploadSink.done)
    .catchError(controller.addError);

    return new ResumableUploadSink._(controller.sink);
  }

  Future<List<int>> read([int start, int end]) {
    throw new UnimplementedError("RemoteFile.read");
  }

  @override
  Future<RemoteFile> delete({bool recursive: false}) =>
      filesystem.connection.deleteObject(
          filesystem.bucket,
          name)
      .then((_) => this);

  Future<int> get length =>
      filesystem.connection.getObject(filesystem.bucket, name, selector: "size")
      .then((result) => result.size);

  String toString() => "RemoteFile: $name";
}

class ResumableUploadSink implements StreamSink<List<int>> {
  final StreamSink<List<int>> _sink;

  ResumableUploadSink._(this._sink);

  @override
  void add(List<int> event) => _sink.add(event);

  @override
  void addError(errorEvent, [StackTrace stackTrace]) => _sink.addError(errorEvent, stackTrace);

  @override
  Future addStream(Stream<List<int>> stream) => _sink.addStream(stream);

  @override
  Future close() => _sink.close();

  @override
  Future get done => _sink.done;
}

