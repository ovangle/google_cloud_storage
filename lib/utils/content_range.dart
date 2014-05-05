library content_range;

import 'package:quiver/core.dart' show hash2;

/**
 * Represents the content of a Http 1.1 Content-Range header.
 */
class ContentRange {
  static final _CONTENT_RANGE =
      new RegExp(
          r'bytes (([0-9]+)-([0-9]+)|\*)\/(([0-9]+|\*))$',
          caseSensitive: false
      );

  static ContentRange parse(String contentRange) {
    var match = _CONTENT_RANGE.matchAsPrefix(contentRange);
    if (match == null) {
      throw new FormatException("Invalid content range: $contentRange");
    }
    var length = match.group(4) != '*' ? int.parse(match.group(4)) : null;
    if (match.group(1) == '*') {
      return new ContentRange(null, length);
    } else {
      int lo = int.parse(match.group(2));
      int hi = int.parse(match.group(3));
      return new ContentRange(new Range(lo, hi), length);
    }
  }

  final Range range;
  final int length;

  ContentRange(this.range, this.length);

  bool operator ==(Object other) =>
      other is ContentRange &&
      other.range == range &&
      other.length == length;

  int get hashCode => hash2(range, length);

  String toString() {
    StringBuffer sbuf = new StringBuffer()
        ..write('bytes ')
        ..write(range != null ? range : '*')
        ..write('/')
        ..write(length != null ? length : '*');
    return sbuf.toString();
  }
}

/**
 * A representation of a Http Range header.
 */
class Range {
  static final Pattern _RANGE = new RegExp(r'([0-9]+)-([0-9]+)$');
  static Range parse(String range) {
    var match = _RANGE.matchAsPrefix(range);
    if (match == null) {
      throw new FormatException(range);
    }
    var lo = int.parse(match.group(1));
    var hi = int.parse(match.group(2));
    return new Range(lo, hi);
  }

  final int lo;
  final int hi;

  Range(this.lo, this.hi) {
    if (lo == null || hi == null)
      throw new ArgumentError("Invalid range");
  }

  bool operator ==(Object other) =>
      other is Range && lo == other.lo && hi == other.hi;

  int get hashCode => hash2(lo, hi);

  String toString() => "$lo-$hi";
}