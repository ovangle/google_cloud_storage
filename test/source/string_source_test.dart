library string_source_test;

import 'package:unittest/unittest.dart';
import 'package:crypto/crypto.dart';

import '../../lib/source/source_common.dart';

void main() {
  group("string source", () {
    test("should be able to set the position", () {
      var source = new StringSource("hello", 'text/plain');
      source.setPosition(3);
      expect(source.position, 3);
    });
    test("should throw when setting an invalid position", () {
      var source = new StringSource("hello", 'text/plain');
      expect(() => source.setPosition(-1), throws);
      expect(() => source.setPosition(10), throws);
    });

    test("should be able to read a given number of bytes from a source", () {
      var source = new StringSource("hello", 'text/plain');
      source.setPosition(2);
      source.read(4)
      .then((bytes) => expect(bytes, [0x6c, 0x6c, 0x6f]));
    });
  });
}