library source_client;

import 'dart:async';
import 'dart:html';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:quiver/async.dart' show forEachAsync, AsyncAction;

import 'source_common.dart';

export 'source_common.dart';

/**
 * A [Source] which reads data from a html [Blob] (eg. a html [File] input)
 */
class BlobSource implements Source {
  // The size of the buffer to use when calculating the hash.
  static const int _BUFFER_SIZE = 256 * 1024;

  Blob blob;

  /**
   * A file reader for reading slices of the blob.
   */
  FileReader reader = new FileReader();
  int _pos = 0;

  final String contentType;

  BlobSource(Blob this.blob, this.contentType);

  @override
  int get length => blob.size;

  @override
  int get position => _pos;

  @override
  Future<List<int>> read(int bytes) {
    return new Future.sync(() {
      var currBlob = blob.slice(_pos, math.min(length, _pos + bytes));

      Completer completer = new Completer();

      var loadSubscription;
      loadSubscription = reader.onLoad.listen((ProgressEvent evt) {
        var result = (evt.target as FileReader).result;
        completer.complete(new Uint8List.view(result));
        loadSubscription.cancel();
      });

      var errSubscription;
      errSubscription = reader.onError.listen((evt) {
        completer.completeError(evt);
        errSubscription.cancel();
      });

      reader.readAsArrayBuffer(currBlob);

      return completer.future;
    });
  }

  @override
  void setPosition(int position) {
    if (position < 0 || position >= length) {
      throw new RangeError.value(position);
    }
    _pos = position;
  }
}

// FIXME: Workaround for quiver bug #125
// forEachAsync does not complete when iterable is empty
Future _forEachAsync(Iterable iterable, AsyncAction action) {
  if (iterable.isEmpty) return new Future.value();
  return forEachAsync(iterable, action);
}