library object_transfer_test;

import 'dart:async';
import 'dart:convert' show UTF8, JSON;

import 'package:unittest/unittest.dart';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

import '../../lib/api/api.dart';
import '../../lib/connection/connection.dart';
import '../../lib/source/source_common.dart';
import '../../lib/utils/content_range.dart';


void main() {
  group("object upload", () {
    test("should be able to successfully initialise an upload", () {
      Future<http.StreamedResponse> streamHandler(http.BaseRequest request, http.ByteStream bodyStream) {
        expect(request.headers['X-UploadContent-Type'], 'text/plain');
        expect(request.headers['X-Upload-Content-Length'], '26');
        expect(request.headers['X-Upload-Content-MD5'], 'w/zT12GS5AB9+0lsymfhOw==');
        expect(request.headers['content-type'], 'application/json; charset=UTF-8');

        expect(request.url.queryParameters['uploadType'], 'resumable');

        return bodyStream.expand((i) => i).toList().then((bytes) {
          expect(
              JSON.decode(UTF8.decode(bytes)),
              { 'bucket': 'bucket', 'name': 'object', 'metadata': {'hello':'world'} }
          );
        }).then((_) {
          return new http.StreamedResponse(
              new Stream.fromIterable([]),
              200,
              headers: { 'location': 'http://example.com'}
          );
        });
      }

      MockClient mockClient = new MockClient.streaming(streamHandler);

      Future<http.BaseResponse> sendAuthorisedRequest(http.BaseRequest request) {
        return mockClient.send(request);
      }

      var connection = new Connection('proj_id', sendAuthorisedRequest);

      var obj = new StorageObject("bucket", "object")
          ..metadata['hello'] = 'world';
      var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
      return connection.uploadObject("bucket", obj, 'text/plain', src).then((resumeToken) {
        expect(resumeToken.uploadUri, Uri.parse("http://example.com"));
        expect(resumeToken.range, new Range(0,-1));
      });
    });

    test("upload should retry an upload with a RETRY status", () {
      var retryCount = 0;

      Future<http.StreamedResponse> streamHandler(http.BaseRequest request, http.ByteStream bodyStream) {
        var response = new http.StreamedResponse(
            new Stream.fromIterable([]),
            (retryCount++ < 2) ? 408: 200,
            headers: {'location': 'http://example.com' }
        );
        return new Future.value(response);
      }

      var connection = new Connection(
          'proj_id',
          (http.BaseRequest request) {
            return new MockClient.streaming(streamHandler).send(request);
          }
      );

      //connection.logger.onRecord.listen(print);

      var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
      return connection.uploadObject("bucket", "object", "text/plain", src).then((resumeToken) {
        expect(resumeToken.uploadUri, Uri.parse("http://example.com"));
        expect(resumeToken.range, new Range(0,-1));
      });
    });

    test("should be able to get the status of an upload", () {
      Future<http.StreamedResponse> streamHandler(http.BaseRequest request, http.ByteStream stream) {
        expect(request.headers['content-range'], 'bytes */26');
        var response = new http.StreamedResponse(
            new Stream.fromIterable([]),
            308,
            headers: {'range': 'bytes=0-13'}
        );
        return new Future.value(response);
      }

      var connection = new Connection(
          'proj_id',
          (http.BaseRequest request) {
            return new MockClient.streaming(streamHandler).send(request);
          }
      );

      var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
      return connection.getUploadStatus(
          new ResumeToken(
              ResumeToken.TOKEN_INTERRUPTED,
              Uri.parse('http://example.com'),
              new Range(0,13),
              null,
              '*'
          ),
          src
      ).then((resumeToken) {
        expect(resumeToken.range, new Range(0,13));
        expect(resumeToken.uploadUri, Uri.parse("http://example.com"));
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
          headers = { 'range': 'bytes=0-${(retryCount * 12)}' };
          status = 308; //Partial content.
        } else {
          headers = { 'range': 'bytes=0-25' };
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
            new Stream.fromIterable([responseBody]),
            status,
            headers: headers
            );
      });
    }

    var connection = new Connection(
        'proj_id',
        (http.BaseRequest request) {
          return new MockClient.streaming(streamHandler).send(request);
        }
    );

    connection.logger.onRecord.listen(print);

    var src = new StringSource("abcdefghijklmnopqrstuvwxyz");
    return connection.resumeUpload(
        new ResumeToken(
            ResumeToken.TOKEN_INIT,
            Uri.parse('http://www.example.com'),
            new Range(0,0),
            null,
            '*'),
        src)
        .then((obj) {
          print(obj);
        });

  });

}