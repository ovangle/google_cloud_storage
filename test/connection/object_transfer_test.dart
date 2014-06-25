library object_transfer_test;

import 'dart:async';
import 'dart:convert' show UTF8, JSON;

import 'package:unittest/unittest.dart';
import 'package:mock/mock.dart';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

import 'package:google_cloud_storage/connection/rpc.dart';
import 'package:google_cloud_storage/connection/resume_token.dart';

import '../../lib/api/api.dart';
import '../../lib/connection/connection.dart';
import '../../lib/source/source_common.dart';
import '../../lib/utils/content_range.dart';


void main() {
  group("object upload", () {
    test("should be able to successfully initialise an upload", () {
      var requestCount = 0;
      Future<http.StreamedResponse> streamHandler(http.BaseRequest request, http.ByteStream bodyStream) {
        if (requestCount++ == 0) {
          expect(request.headers['X-Upload-Content-Type'], 'text/plain');
          expect(request.headers['X-Upload-Content-Length'], '26');
          expect(request.headers['content-type'], 'application/json; charset=utf-8');
          expect(request.url.queryParameters['uploadType'], 'resumable');

          return bodyStream.expand((i) => i).toList().then((bytes) =>
              expect(JSON.decode(UTF8.decode(bytes)), {
                'bucket': 'bucket', 'name': 'object', 'metadata': { 'hello':'world' }})).then((_) =>
                    new http.StreamedResponse(new Stream.fromIterable([]), 200,
                        headers: { 'location': 'http://example.com' }));
        } else {
          return bodyStream.expand((i) => i).toList().then(expectAsync((bytes) {
            expect(bytes, UTF8.encode("abcdefghijklmnopqrstuvwxyz"));
          })).then((_) {
            Iterable<int> stream = UTF8.encode(JSON.encode({'bucket':'bucket', 'name':'object'}));
            return new http.StreamedResponse( new Stream.fromIterable([stream]), 200,
                headers: {'content-type' : 'application/json; charset=UTF-8'});
          });
        }
      }

      var connection = new Connection(
          'proj_id',
          new MockRpcClient(streamHandler)
      );

      var obj = new StorageObject("bucket", "object")
          ..metadata['hello'] = 'world';
      var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
      return connection.uploadObject("bucket", obj, 'text/plain', src).then(expectAsync((resumeToken) {
        expect(resumeToken.uploadUri, Uri.parse("http://example.com"));
        expect(resumeToken.range, null);
        expect(resumeToken.done != null, true);
        resumeToken.done.then(expectAsync((StorageObject so) {
          expect(so.bucket, 'bucket');
          expect(so.name, 'object');
        }));
      }));
    });

    test("upload should retry an upload with a RETRY status", () {
      var retryCount = 0;

      Future<http.StreamedResponse> streamHandler(http.BaseRequest request, http.ByteStream bodyStream) {
        var response = new http.StreamedResponse(
            new Stream.fromIterable([]),
            (retryCount++ < 2) ? 504: 200,
            headers: {'location': 'http://example.com' }
        );
        return new Future.value(response);
      }

      var connection = new Connection(
          'proj_id',
          new MockRpcClient(streamHandler)
      );

      //connection.logger.onRecord.listen(print);

      var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
      return connection.uploadObject("bucket", "object", "text/plain", src).then((resumeToken) {
        expect(resumeToken.uploadUri, Uri.parse("http://example.com"));
        expect(resumeToken.range, null);
      });
    });

  });

  test("should be able to upload a file", () {
    var retryCount = 0;
    Future<http.StreamedResponse> streamHandler(http.BaseRequest request, http.ByteStream bodyStream) {

      var responseBody = [];
      var status, headers;
      bool testBody = false;
      var contentRange = request.headers['content-range'];
      expect(contentRange, isNotNull);
      contentRange = ContentRange.parse(contentRange);
      if (contentRange.range == null) {
        //Upload status.
        if (retryCount++ < 1) {
          headers = { 'range': '0-${(retryCount * 12)}' };
          status = 308; //Partial content.
        } else {
          headers = { 'range': '0-25' };
          status = 200;
        }
      } else {
        testBody = false;
        //Actual upload content.
        if (retryCount < 1) {
          headers = {};
          status = 503;
        } else {
          headers = {'content-type': 'application/json'};
          responseBody = UTF8.encode(JSON.encode({'bucket':'bucket', 'name':'object'}));
          status = 200;
        }
      }
      return bodyStream.expand((i) => i).toList().then((bytes) {
        if (headers.isEmpty && retryCount < 2) {
          expect(bytes, UTF8.encode("abcdefghijklmnopqrstuvwxyz".substring(retryCount * 12)));
        }
      }).then((_) {
        print("Response body: $responseBody");
        return new http.StreamedResponse(
            new Stream.fromIterable([responseBody]), status, headers: headers);
      });
    }

    var connection = new Connection('proj_id', new MockRpcClient(streamHandler));

    connection.logger.onRecord.listen(print);

    var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
    return connection.resumeUpload(
        new ResumeToken(Uri.parse('http://www.example.com'), selector: '*'), src)
            .then((obj) {
              print(obj);
            });

  });

  test("should be able to resume an upload from a serialized resume token", () {
    var requestCount = 0;
    Future<http.StreamedResponse> streamHandler(http.BaseRequest request, http.ByteStream bodyStream) {
      if (requestCount++ == 0) {
        expect(request.headers['authorization'], isNotNull);
        expect(request.headers['content-range'], 'bytes */26');
        // TODO (@marko) Find out if not setting length to 0 can cause issues.
        // expect(request.headers['content-length'], isZero);
        return new Future.value(new http.StreamedResponse(new Stream.fromIterable([]), 308,  headers: {
          'range': '0-12',
          'content-length': '0',
        }));
      } else {
        expect(request.headers['authorization'], isNotNull);
        expect(request.headers['content-range'], 'bytes 13-25/26');
        bodyStream.expand((i) => i).toList().then((bytes) {
          expect(bytes, UTF8.encode("nopqrstuvwxyz"));
        });
        var mockResponse = UTF8.encode(JSON.encode({"name": "test", "bucket": "test-bucket"}));
        return new Future.value(new http.StreamedResponse(new Stream.fromIterable([mockResponse]), 200,
            headers: {'content-type': 'application/json'}));
      }
    }

    var connection = new Connection('proj_id', new MockRpcClient(streamHandler));

    connection.logger.onRecord.listen(print);

    var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
    return connection.resumeUpload(new ResumeToken.fromJson({'uploadUri': 'http://www.session.uri', 'selector': '*'}),
        src).then(expectAsync((StorageObject so) {
          expect(so.bucket, "test-bucket");
          expect(so.name, "test");
        }));
  });

}

class MockRpcClient extends RpcClient implements Mock {

  final http.BaseClient baseClient;

  MockRpcClient._(baseClient, streamHandler): this.baseClient = baseClient, super(baseClient) {
  }

  factory MockRpcClient(streamHandler) {
    return new MockRpcClient._(new MockClient.streaming(streamHandler), streamHandler);
  }

  Future<RpcRequest> authorize(RpcRequest request) {
    request.headers['Authorization'] = 'Bearer someToken';
    return new Future.value(request);
  }

}