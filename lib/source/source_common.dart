/**
 * The [Source] interface and [Source] implementations which are common to both
 * server and client.
 */
library source_common;


import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

import 'package:crypto/crypto.dart' show MD5;

/**
 * An interface which represents an object from which
 * a fixed number of bytes can be read.
 */
abstract class Source {

  /**
   * Get the length of the [Source] in bytes
   */
  int get length;

  /**
   * The current position in the source.
   */
  int get position;

  /**
   * An optional hash value used to verify the upload.
   * Can be `null` if the upload need not be validated.
   */
  Future<List<int>> md5();

  /**
   * Read a given number of bytes from the [Source].
   *
   * If there are less than [:bytes:] left in the source,
   * then the resulting list will contain only those bytes.
   */
  Future<List<int>> read(int bytes);

  /**
   * Set the position in the [SearchableSource] at which
   * to resume the upload.
   */
  void setPosition(int position);
}

/**
 * A [Source] which contains the bytes of a [String]
 */
class StringSource implements Source {

  final Encoding _encoding;
  final String source;
  int _pos = 0;

  StringSource(this.source, [String encoding = 'utf-8']):
    this._encoding = Encoding.getByName(encoding);


  @override
  int get length => source.length;

  @override
  Future<List<int>> md5() {
    return new Future.sync(() {
      var hash = new MD5();
      hash.add(_encoding.encode(source));
      return hash.close();
    });
  }

  @override
  int get position => _pos;

  @override
  Future<List<int>> read(int bytes) {
    return new Future.sync(() {
      var content = source.substring(_pos, math.min(_pos + bytes, length));
      return _encoding.encode(content);
    });
  }

  @override
  void setPosition(int position) {
    if (position < 0 || position >= length) {
      throw new RangeError.range(position, 0, length - 1);
    }
    _pos = position;
  }
}
