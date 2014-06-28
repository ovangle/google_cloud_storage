/**
 * The [Source] interface and [Source] implementations which are common to both
 * server and client.
 */
library source_common;


import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

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
   * The mime type of the source
   */
  String get contentType;

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
 * A [Source] which is backed by a [String].
 */
class StringSource extends ByteSource {

  StringSource(String source, String contentType, [String encoding = 'utf-8']):
    super(Encoding.getByName(encoding).encode(source), contentType);
}

/**
 * A [Source] which is backed by a list of bytes.
 */
class ByteSource implements Source {
  final List<int> source;
  int _pos = 0;

  final String contentType;

  ByteSource(this.source, this.contentType);

  @override
  int get length => source.length;

  @override
  int get position => _pos;

  @override
  Future<List<int>> read(int bytes) =>
      new Future.value(
          source.sublist(_pos, math.min(_pos + bytes, length))
      );

  @override
  void setPosition(int position) {
    if (position < 0 || position >= length) {
      throw new RangeError.range(position, 0, length - 1);
    }
    _pos = position;
  }
}
