library connection.rpc_test;

import 'package:unittest/unittest.dart';

import 'package:google_cloud_storage/connection/rpc.dart';

void main() {
  group('rpc', () {

    group('multipart rpc', () {
      test('should be a properly formatted multipart/related request', () {
        var rpcRequest = new MultipartRelatedRpcRequest(
            Uri.parse('http://www.example.com'),
            method: 'POST'
        );

        var part1 = new RpcRequestPart('application/json charset=UTF-8')
            ..jsonBody = { 'name': 'myObject' };

        var part2 = new RpcRequestPart('text/plain')
            ..body = 'hello world';

        rpcRequest.requestParts = [part1, part2];


        var request = rpcRequest.asRequest();

        expect(request.headers['content-type'], 'multipart/related; boundary="multipart_boundary"');
        expect(request.body,
            "--multipart_boundary\r\n"
            "content-type: application/json charset=UTF-8\r\n"
            "\r\n"
            '{"name":"myObject"}\r\n'
            "--multipart_boundary\r\n"
            "content-type: text/plain\r\n"
            "\r\n"
            "hello world\r\n"
            "--multipart_boundary--\r\n"
        );
      });
    });
  });
}

