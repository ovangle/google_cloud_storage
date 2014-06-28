library bucket_test;

import 'dart:async';
import 'dart:convert' show UTF8, JSON;

import 'package:unittest/unittest.dart';

import 'package:http/http.dart' as http;

import '../../lib/api/api.dart';
import '../../lib/connection/connection.dart';

import 'mock_rpc_client.dart';

void main() {
  group("get bucket", () {
    var returnStatus;
    var connection;

    Future<http.StreamedResponse> getObjectHandler(http.BaseRequest request, http.ByteStream bodyStream) {
      expect(request.method, 'GET');

      var fields = request.url.queryParameters['fields'];
      var responseBucket = new StorageBucket('example-bucket', selector: fields);
      var responseBody = JSON.encode(responseBucket);

      http.StreamedResponse response = new http.StreamedResponse(
          new Stream.fromIterable([UTF8.encode(responseBody)]),
          returnStatus,
          headers: {'content-type': 'application/json; charset=utf-8'}
      );
      return new Future.value(response);
    }

    setUp(() {
      connection =  new Connection(
          'proj_id',
          new MockRpcClient(getObjectHandler)
      );
    });


    test("should be able to retrieve a bucket from storage", () {
      returnStatus = 200;

      return connection.getBucket("example-bucket")
          .then((bucket) {

        expect(bucket.name, 'example-bucket');
        expect(bucket.selector, '*');
      });
    });

    test("should be able to request a partial bucket", () {
      returnStatus = 200;

      return connection.getBucket("example-bucket",
          queryParams: { 'fields': 'name'}).then((bucket) {
        expect(bucket.hasField('name'), isTrue);
        expect(bucket.hasField('logging'), isFalse);
      });
    });
  });


}