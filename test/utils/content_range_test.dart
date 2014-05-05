library content_range_test;

import 'package:unittest/unittest.dart';
import '../../lib/utils/content_range.dart';

void main() {
  group("content range", () {
    group("parsing", () {
      test("parse 'bytes */*'", () {
        expect(ContentRange.parse("bytes */*"),new ContentRange(null, null));
      });
      test("parse 'bytes */100000", () {
        expect(ContentRange.parse("bytes */100000"), new ContentRange(null, 100000));
      });
      test("parse 'bytes 0-500/10000", () {
        expect(ContentRange.parse('bytes 0-500/10000'), new ContentRange(new Range(0,500), 10000));
      });
    });
  });

}