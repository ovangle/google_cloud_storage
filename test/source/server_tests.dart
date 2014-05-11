library server_tests;

import 'string_source_test.dart' as string_source;
import 'file_source_test.dart' as file_source;

void main() {
  string_source.main();
  file_source.main();
}