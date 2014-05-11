library server_tests;

import 'json/all_tests.dart' as json;
import 'source/server_tests.dart' as source;
import 'utils/all_tests.dart' as utils;


void main() {
  json.main();
  source.main();
  utils.main();

}