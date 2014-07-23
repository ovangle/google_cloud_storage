library testing_server;

import '../cloud_storage_server.dart';
import 'testing_common.dart';

class MockStorageConnection extends MockConnection
implements CloudStorageConnection {}