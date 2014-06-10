library server_tests;

import 'connection/all_tests.dart' as connection;
import 'json/all_tests.dart' as json;
import 'source/server_tests.dart' as source;
import 'utils/all_tests.dart' as utils;


void main() {
  connection.main();
  json.main();
  source.main();
  utils.main();

}