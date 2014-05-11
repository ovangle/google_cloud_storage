/**
 * Implementations of [Source] that are dependent on the
 * `dart:io` library.
 */
library source_server;

import 'dart:async';
import 'dart:io';

import 'package:crypto/crypto.dart';

import 'source_common.dart';

export 'source_common.dart';

class FileSource implements Source {
  File file;

  int _pos = 0;

  FileSource(this.file);

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
  Future<List<int>> md5() {
    var md5 = new MD5();
    var completer = new Completer();

    file.openRead().listen(
        md5.add,
        onError: completer.completeError,
        cancelOnError: true,
        onDone: () => completer.complete(md5.close())
    );

    return completer.future;
  }

  @override
  int get position => _pos;

  @override
  Future<List<int>> read(int bytes) =>
      file.open(mode: FileMode.READ)
          .then((f) =>
              f.read(bytes).whenComplete(() => f.close())
          );

  @override
  void setPosition(int position) {
    if (position < 0 || position >= length) {
      throw new RangeError.value(position);
    }
    _pos = position;
  }
}