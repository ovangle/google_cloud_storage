library mock_connection;

import 'dart:async';
import 'package:mock/mock.dart';

import '../lib/utils/either.dart';
import '../lib/api/api.dart';
import '../lib/connection/connection.dart';

class MockConnection extends Mock implements CloudStorageConnection {

  List<Map<String,dynamic>> buckets = [];
  List<Map<String,dynamic>> objects = [];

  void clearData() {
    buckets = [];
    objects = [];
  }

  @override
  Future<StorageObject> getObject(bucket, String name, {int generation, int ifGenerationMatch, int ifGenerationNotMatch, int ifMetagenerationMatch, int ifMetagenerationNotMatch, String projection: 'noAcl', String selector: "*"}) {
    return new Future.sync(() {
      for (var obj in objects) {
        if (obj['bucket'] == bucket && obj['name'] == name) {
          return new StorageObject(bucket, name);
        }
      }
      throw new MockRPCException.notFound();
    });
  }

  Stream<Either<String,StorageObject>> listBucketContents(
      var bucket, String prefix,
      { int maxResults: -1,
        String projection: 'noAcl',
        String delimiter: "/",
        bool versions: false,
        String selector: "*"
      }) {
    StreamController controller = new StreamController<Either<String,StorageObject>>();
    Set<String> prefixes = new Set();
    for (var obj in objects) {
      if (obj['bucket'] == bucket && obj['name'].startsWith(prefix)) {
        String name = obj['name'];
        var nextDelim = name.indexOf(delimiter, prefix.length);
        if (nextDelim < 0) {
          controller.add(new Either.ofRight(new StorageObject(obj['bucket'], obj['name'])));
        } else {
          prefixes.add(name.substring(0, nextDelim));
        }
      }
    }
    for (prefix in prefixes) {
      controller.add(new Either.ofLeft(prefix));
    }
    controller.close();
    return controller.stream;
  }

}

class MockRPCException extends Mock implements RPCException {
  final int statusCode;

  MockRPCException.notFound():
    this.statusCode = 404;
}
