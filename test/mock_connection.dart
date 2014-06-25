library mock_connection;

import 'dart:async';
import 'package:mock/mock.dart';
import 'package:http/http.dart' as http;
import 'package:logging/logging.dart';

import '../lib/utils/either.dart';
import '../lib/utils/content_range.dart';
import '../lib/api/api.dart';
import '../lib/connection/connection.dart';
import '../lib/source/source_common.dart';

class MockConnection extends Mock implements Connection {

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


  @override
  void set logger(Logger _logger) {
    throw new UnsupportedError('MockConnection.logger');
  }

  @override
  Logger get logger => throw new UnsupportedError('MockConnection.logger');

  @override
  int get maxRetryRequests => 0;

  @override
  void set maxRetryRequests(int _maxRetryRequests) {
    throw new UnsupportedError('set MockConnection.maxRetryRequests');
  }

  @override
  String get projectId => null;

  @override
  Future<StorageObject> composeObjects(String destinationBucket, destinationObject, List sourceObjects, {destinationPredefinedAcl: PredefinedAcl.PROJECT_PRIVATE, ifGenerationMatch, ifMetagenerationMatch, String selector: '*'}) {
    // TODO: implement composeObjects
  }

  @override
  Future<StorageObject> copyObject(String sourceBucket, String sourceObject, String destinationBucket, destinationObject, {int sourceGeneration, int ifSourceGenerationMatch, int ifSourceGenerationNotMatch, int ifSourceMetagenerationMatch, int ifSourceMetagenerationNotMatch, int ifDestinationGenerationMatch, int ifDestinationGenerationNotMatch, int ifDestinationMetagenerationMatch, int ifDestinationMetagenerationNotMatch, String projection: 'noAcl', PredefinedAcl destinationPredefinedAcl: PredefinedAcl.PROJECT_PRIVATE, String selector: '*'}) {
    // TODO: implement copyObject
  }

  @override
  Future<StorageBucket> createBucket(bucket, {String projection: 'noAcl', String selector: '*'}) {
    // TODO: implement createBucket
  }

  @override
  Future deleteBucket(String bucket, {int ifMetagenerationMatch, int ifMetagenerationNotMatch}) {
    // TODO: implement deleteBucket
  }

  @override
  Future deleteObject(String bucket, String object, {int generation, int ifGenerationMatch, int ifGenerationNotMatch, int ifMetagenerationMatch, int ifMetagenerationNotMatch}) {
    // TODO: implement deleteObject
  }

  @override
  Stream<List<int>> downloadObject(String bucket, String object, {int generation, int ifGenerationMatch, int ifGenerationNotMatch, int ifMetagenerationMatch, int ifMetagenerationNotMatch, Range byteRange}) {
    // TODO: implement downloadObject
  }

  @override
  Future<StorageBucket> getBucket(String name, {int ifMetagenerationMatch, int ifMetagenerationNotMatch, String projection: 'noAcl', String selector: "*"}) {
    // TODO: implement getBucket
  }

  @override
  Future<ResumeToken> _getUploadStatus(Uri uploadUri, Source source) {
    // TODO: implement getUploadStatus
  }

  @override
  Stream<Either<String, StorageObject>> listBucket(String bucket, {String prefix, String delimiter, int maxResults: -1, String projection: 'noAcl', bool versions: false, String selector: "*"}) {
    // TODO: implement listBucket
  }

  @override
  Stream<StorageBucket> listBuckets({int maxResults: -1, String projection: 'noAcl', String selector: '*'}) {
    // TODO: implement listBuckets
  }

  @override
  Future<StorageObject> resumeUpload(ResumeToken resumeToken) {
    // TODO: implement resumeUpload
  }

  @override
  Future<StorageBucket> updateBucket(String bucket, void modify(StorageBucket bucket), {int ifMetagenerationMatch, int ifMetagenerationNotMatch, String projection, String readSelector: "*", String resultSelector}) {
    // TODO: implement updateBucket
  }

  @override
  Future<StorageObject> updateObject(String bucket, String object, void modify(StorageObject object), {int generation, int ifGenerationMatch, int ifGenerationNotMatch, int ifMetagenerationMatch, int ifMetagenerationNotMatch, PredefinedAcl predefinedAcl: PredefinedAcl.PRIVATE, String projection: 'noAcl', String readSelector: '*', String resultSelector}) {
    // TODO: implement updateObject
  }

  @override
  Future<ResumeToken> uploadObject(String bucket, object, String mimeType, Source source, {int ifGenerationMatch, int ifGenerationNotMatch, int ifMetagenerationMatch, int ifMetagenerationNotMatch, PredefinedAcl predefinedAcl: PredefinedAcl.PROJECT_PRIVATE, String projection: 'noAcl', String selector: '*'}) {
    // TODO: implement uploadObject
  }
}

class MockRPCException extends Mock implements RPCException {
  final int statusCode;

  MockRPCException.notFound():
    this.statusCode = 404;


  @override
  String get message => null;

  @override
  String get method => null;

  @override
  http.BaseResponse get response => null;

  @override
  Uri get url => null;
}
