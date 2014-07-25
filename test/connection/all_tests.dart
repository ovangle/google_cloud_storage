library connection_tests;

import 'package:unittest/unittest.dart';
import 'object_transfer_test.dart' as object_transfer;
import 'rpc_test.dart' as rpc;

void main() {
  group('connection', () {
    rpc.main();
    object_transfer.main();
  });

}