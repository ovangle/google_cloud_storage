library blob_source_test;

import 'dart:async';
import 'dart:convert' show UTF8;
import 'dart:html';
import 'dart:typed_data';

import 'package:mock/mock.dart';
import 'package:unittest/unittest.dart';

import 'package:crypto/crypto.dart';

import '../../lib/source/source_client.dart';


void main() {
  var source = new BlobSource(new MockBlob("hello"), 'text/plain')
      ..reader = new MockFileReader();

  group("blob source", () {
    test("should be able to set the position", () {
      source.setPosition(3);
      expect(source.position, 3);
    });
    test("should throw when setting an invalid position", () {
      expect(() => source.setPosition(-1), throws);
      expect(() => source.setPosition(10), throws);
    });

    test("should be able to get the md5 hash of the source", () {
      return source.md5().then((bytes) {
        expect(CryptoUtils.bytesToBase64(bytes), "XUFAKrxLKna5cZ2REBfFkg==");
      });
    });

    test("should be able to read a given number of bytes from a source", () {
      source.setPosition(2);
      source.read(4)
      .then((bytes) => expect(bytes, [0x6c, 0x6c, 0x6f]));
    });
  });
}

class MockBlob extends Mock implements Blob {

  String input;

  MockBlob(this.input);

  Blob slice([int start, int end, String contentType]) {
    if (end != null) {
      return new MockBlob(input.substring(start, end));
    }
    if (start != null) {
      return new MockBlob(input.substring(start));
    }
    return new MockBlob(input);
  }

  int get size => input.length;

  @override
  String get type => null;
}

class MockFileReader extends Mock implements FileReader {
  final onLoadController = new StreamController.broadcast();
  Stream get onLoad => onLoadController.stream;

  final onErrorController = new StreamController.broadcast();
  Stream get onError => onErrorController.stream;

  void readAsArrayBuffer(MockBlob blob) {
    var bytes = new Uint8List.fromList(UTF8.encode(blob.input));
    result = bytes.buffer;
    new Future.delayed(new Duration(),
        () => onLoadController.add(
        new MockProgressEvent()..target = this)
    );
  }

  ByteBuffer result;

  @override
  void abort() {
    throw new UnimplementedError(' abort');
  }

  @override
  void addEventListener(String type, listener(Event event), [bool useCapture]) {
    throw new UnimplementedError(' addEventListener');
  }

  @override
  bool dispatchEvent(Event event) {
    throw new UnimplementedError(' dispatchEvent');
  }

  @override
  FileError get error => null;

  @override
  Events get on => null;

  @override
  Stream<ProgressEvent> get onAbort => null;

  @override
  Stream<ProgressEvent> get onLoadEnd => null;

  @override
  Stream<ProgressEvent> get onLoadStart => null;

  @override
  Stream<ProgressEvent> get onProgress => null;

  @override
  void readAsDataUrl(Blob blob) {
    throw new UnimplementedError(' readAsDataUrl');
  }

  @override
  void readAsText(Blob blob, [String encoding]) {
    throw new UnimplementedError(' readAsText');
  }

  @override
  int get readyState => null;

  @override
  void removeEventListener(String type, listener(Event event), [bool useCapture]) {
    throw new UnimplementedError(' removeEventListener');
  }
}

class MockProgressEvent extends Mock implements ProgressEvent {
  FileReader target;

  @override
  bool get bubbles => null;

  @override
  bool get cancelable => null;

  @override
  DataTransfer get clipboardData => null;

  @override
  EventTarget get currentTarget => null;

  @override
  bool get defaultPrevented => null;

  @override
  int get eventPhase => null;

  @override
  bool get lengthComputable => null;

  @override
  int get loaded => null;

  @override
  Element get matchingTarget => null;

  @override
  List<Node> get path => null;

  @override
  void preventDefault() {
    throw new UnsupportedError('preventDefault');
  }

  @override
  void stopImmediatePropagation() {
    throw new UnimplementedError(' stopImmediatePropagation');
  }

  @override
  void stopPropagation() {
    throw new UnimplementedError('stopPropagation');
  }

  @override
  int get timeStamp => null;

  @override
  int get total => null;

  @override
  String get type => null;
}