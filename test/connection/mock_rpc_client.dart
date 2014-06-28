library mock_rpc_client;

import 'dart:async';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import '../../lib/connection/rpc.dart';


class MockRpcClient extends RpcClient {

  final http.BaseClient baseClient;

  MockRpcClient._(baseClient, streamHandler): this.baseClient = baseClient, super(baseClient) {
  }

  factory MockRpcClient(streamHandler) {
    return new MockRpcClient._(new MockClient.streaming(streamHandler), streamHandler);
  }

  Future<BaseRpcRequest> authorize(BaseRpcRequest request) {
    request.headers['Authorization'] = 'Bearer someToken';
    return new Future.value(request);
  }

}