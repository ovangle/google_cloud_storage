library file_source_test;

import 'dart:io';

import 'package:unittest/unittest.dart';
import 'package:crypto/crypto.dart';
import '../../lib/source/source_server.dart';

void main() {
  group("file source", () {
    var testFile = new File('file_source_test.json');
    setUp(() {
      testFile.writeAsStringSync("hello");
    });
    tearDown(() {
      testFile.deleteSync();
    });

    test("should be able to set the position", () {
      var source = new FileSource(testFile, 'text/plain');
      source.setPosition(3);
      expect(source.position, 3);
    });
    test("should throw when setting an invalid position", () {
      var source = new FileSource(testFile, 'text/plain');
      expect(() => source.setPosition(-1), throws);
      expect(() => source.setPosition(10), throws);
    });

    test("should be able to read a given number of bytes from a source", () {
      var source = new FileSource(testFile, 'text/plain');
      source.setPosition(2);
      source.read(4)
      .then((bytes) => expect(bytes, [0x6c, 0x6c, 0x6f]));
    });
  });
}