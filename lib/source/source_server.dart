/**
 * Implementations of [Source] that are dependent on the
 * `dart:io` library.
 */
library source_server;

import 'dart:async';
import 'dart:io';

import 'source_common.dart';

export 'source_common.dart';

class FileSource implements Source {
  File file;

  @override
  final String contentType;

  int _pos = 0;

  FileSource(this.file, this.contentType);

  //A cached value for the length of the file.
  int _length;
  @override
  int get length {
    if (_length == null) {
      _length = file.lengthSync();
    }
    return _length;
  }

  @override
  int get position => _pos;

  @override
  Future<List<int>> read(int bytes) =>
      file.open(mode: FileMode.READ)
          .then((f) => f.setPosition(_pos))
          .then((f) => f.read(bytes).whenComplete(() => f.close()));

  @override
  void setPosition(int position) {
    if (position < 0 || position >= length) {
      throw new RangeError.value(position);
    }
    _pos = position;
  }
}