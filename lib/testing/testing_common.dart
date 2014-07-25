library testing;

import 'dart:async';

import 'package:http/http.dart' as http;
import 'package:quiver/async.dart' show forEachAsync;

import '../api/api.dart';
import '../connection/rpc.dart';
import '../connection/connection.dart';
import '../connection/resume_token.dart';
import '../source/source_common.dart';
import '../utils/either.dart';
import '../utils/content_range.dart';

export '../api/api.dart';
export '../source/source_common.dart';


/**
 * Provides an interface which implements connection for use
 * during testing.
 */
class MockConnection implements Connection {
  Map<String, _StoredBucket> _storedBuckets = new Map();

  @override
    Future<StorageBucket> createBucket(bucket, {Map<String, String> params: const {}}) {
      return new Future.value().then((_) {
        var metadata = (bucket is String) ? new StorageBucket(bucket) : new StorageBucket.fromJson(bucket.toJson());
        var name = (bucket is String) ? bucket : bucket.name;
        if (_storedBuckets[name] != null)
          throw 'Bucket $bucket already exists';
        var storedBucket = new _StoredBucket()
          ..metadata = metadata;
        _storedBuckets[name] = storedBucket;
        return metadata;
      });
    }

  Future<_StoredBucket> _getStoredBucket(String name) {
    return new Future.value().then((_) {
      var storedBucket = _storedBuckets[name];
      if (storedBucket == null)
        throw new _MockRpcException(404, 'bucket $name not found');
      return storedBucket;

    });
  }

    @override
    Future<StorageBucket> getBucket(String name, {Map<String, String> params: const {}}) {
      return _getStoredBucket(name).then((storedBucket) => storedBucket.metadata);
    }

    @override
   Future deleteBucket(String bucket, {Map<String, dynamic> params}) {
     return new Future.value()
        .then((_) => _storedBuckets.remove(bucket));
   }

    @override
    Stream<StorageBucket> listBuckets({Map<String, String> params: const {}}) {
      return new Stream.fromIterable(_storedBuckets.values.map((b) => b.metadata));
    }

    @override
    Future<StorageBucket> patchBucket(String bucket, void modify(StorageBucket bucket), {Map<String, String> params: const {}}) {
      return _getStoredBucket(bucket).then((storedBucket) {
        modify(storedBucket.metadata);
        return storedBucket.metadata;
      });
    }

    @override
    Future<StorageBucket> updateBucket(StorageBucket bucket, {Map<String, String> params: const {}}) {
      return _getStoredBucket(bucket.name)
          .then((storedBucket) {
        _storedBuckets[bucket.name].metadata = bucket;
      });
    }

    Future<_StoredObject> _getStoredObject(String bucket, String object) {
      return _getStoredBucket(bucket).then((storedBucket) {
        var storedObject = storedBucket.objects[object];
        if (storedObject == null)
          throw new _MockRpcException(404, '$object not found in bucket $bucket');
        return storedObject;
      });
    }

    @override
    Future<StorageObject> getObject(String bucket, String object, {Map<String, String> params: const {}}) {
      return _getStoredObject(bucket, object)
          .then((storedObject) => storedObject.metadata);
    }

    @override
    Future<ResumeToken> uploadObject(String bucket, object, Source source, {Map<String, String> params: const {}}) {
      return _getStoredBucket(bucket).then((storedBucket) {
        var metadata = (object is String) ? new StorageObject(bucket, object) : new StorageObject.fromJson(object.toJson());
        var objectName = (object is String) ? object : object.name;
        var storedObject = new _StoredObject()
            ..metadata = metadata;
        if (storedBucket.objects[objectName] != null) {
          throw 'Object $objectName already exists in bucket $bucket';
        }
        storedBucket.objects[objectName] = storedObject;

        return source.read(source.length).then((bytes) {

          storedObject.objectData = bytes;

          Completer completer = new Completer();
          completer.complete(metadata);

          return new ResumeToken(
              Uri.parse('http://www.example.com'),
              completer: completer
          );
        });
      });
    }

    @override
    Future<ResumeToken> resumeUpload(ResumeToken resumeToken, Source source) {
      //uploadObject always succeeds. No need to resume.
      throw new UnimplementedError('MockConnection.resumeUpload');
    }

    @override
    Future<StorageObject> composeObjects(String destinationBucket, destinationObject, List sourceObjects, {Map<String, String> params: const {}}) {
      return _getStoredBucket(destinationBucket).then((storedBucket) {
        var objectName = (destinationObject is String) ? destinationObject : destinationObject.name;
        var objectMetadata = (destinationObject is String)
            ? new StorageObject(destinationBucket, destinationObject)
            : new StorageObject.fromJson(destinationObject.toJson());

        if (sourceObjects.any((obj) => obj is! StorageObject))
          throw new UnsupportedError('MockConnection.composeObjects does not support CompositionSources');
        if (storedBucket.objects.containsKey(objectName))
          throw 'Object $objectName already exists in bucket $destinationBucket';

        List<int> destinationData = [];
        return forEachAsync(
            sourceObjects,
            (obj) {
              return _getStoredObject(obj.bucket, obj.name)
                  .then((storedObject) {
                    destinationData.addAll(storedObject.objectData);
                  });
            }).then((_) {
          storedBucket.objects[objectName] = new _StoredObject()
              ..metadata = objectMetadata
              ..objectData = destinationData;
          return objectMetadata;
        });
      });

    }

    @override
    Future<StorageObject> copyObject(String sourceBucket, String sourceObject, String destinationBucket, destinationObject, {Map<String, String> params: const {}}) {
      return _getStoredBucket(destinationBucket).then((destBucket) {
        var destObjectName = (destinationObject is String) ? destinationObject : destinationObject.name;
        var destObjectMetadata = (destinationObject is String)
            ? null
            : new StorageObject.fromJson(destinationObject.toJson());
        if (destBucket.objects[destObjectName] != null)
          throw 'Object $destObjectName already exists in destination bucket $destinationBucket';
        return _getStoredObject(sourceBucket, sourceObject).then((sourceObject) {
          var destObject = new _StoredObject()
              ..metadata = (destObjectMetadata != null)
                  ? destObjectMetadata
                  : sourceObject.metadata
              ..objectData = new List.from(sourceObject.objectData);
          destBucket.objects[destObjectName] = destObject;
          return destObject.metadata;

        });
      });
    }

    @override
    Future deleteObject(String bucket, String object, {Map<String, dynamic> params}) {
      return _getStoredBucket(bucket).then((storedBucket) {
        storedBucket.objects.remove(object);
      });
    }

    @override
    Stream<List<int>> downloadObject(String bucket, String object, {Range range, Map<String, String> params: const {}}) {
      return new Stream.fromFuture(
          _getStoredObject(bucket, object).then((storedObject) => storedObject.objectData)
      );
    }

    @override
    Stream<Either<String, StorageObject>> listObjects(String bucket, {Map<String, String> params: const {}}) {
      StreamController controller = new StreamController();

      _getStoredBucket(bucket).then((storedBucket) {
        var prefix = params.putIfAbsent('prefix', () => '');
        var delimeter = params.putIfAbsent('delimiter', () => null);
        var matchingKeys = storedBucket.objects.keys.where((obj) => obj.startsWith(prefix));

        Set seenFolders = new Set();

        for (var k in matchingKeys) {
          if (delimeter == null) {
            controller.add(new Either.ofRight(storedBucket.objects[k].metadata));
            continue;
          }
          var nextDelimIndex = k.indexOf(delimeter, prefix.length);
          if (nextDelimIndex >= 0) {
            var folderName = k.substring(0, nextDelimIndex + 1);
            var added = seenFolders.add(folderName);
            if (added)
              controller.add(new Either.ofLeft(folderName));
          } else {
            controller.add(new Either.ofRight(storedBucket.objects[k].metadata));
          }
        }
      })
      .then((_) => controller.close())
      .catchError(controller.addError);
      return controller.stream;
    }

    @override
    Future<StorageObject> patchObject(String bucket, String object, void modify(StorageObject object), {Map<String, String> params: const {}}) {
      return _getStoredObject(bucket, object)
          .then((storedObject) {
            modify(storedObject.metadata);
          });
    }


   @override
   void set logger(_logger) {
     throw new UnsupportedError('MockConnection.logger');
   }

   @override
   get logger => throw new UnsupportedError('MockConnection.logger');

   @override
   int get maxRetryRequests => throw new UnsupportedError('MockConnection.maxRetryRequests');

   @override
   void set maxRetryRequests(int _maxRetryRequests) {
     throw new UnsupportedError('MockConnection.maxRetryRequests');
   }

  @override
  String get projectId => null;



  @override
  Future<StorageObject> uploadObjectMultipart(String bucket, object, Source source, {Map<String, String> params: const {}}) {
    return uploadObject(bucket, object, source, params: params)
        .then((token) => token.done);
  }

  @override
  Future<StorageObject> uploadObjectSimple(String bucket, String object, Source source, {Map<String, String> params: const {}}) {
    return uploadObject(bucket, object, source, params: params)
        .then((token) => token.done);
  }
}

class _StoredBucket {
  StorageBucket metadata;
  Map<String, _StoredObject> objects = new Map();
}

class _StoredObject {
  StorageObject metadata;
  List<int> objectData;
}

class _MockRpcException implements RpcException {

  final int statusCode;
  final String message;

  _MockRpcException(this.statusCode, this.message);


  @override
  http.Response get response => throw new UnsupportedError('_MockRpcException.response');

  toString() => 'RpcException ($statusCode) $message';

}