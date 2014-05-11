library client_tests;

import 'package:unittest/html_config.dart';

import 'string_source_test.dart' as string_source;
import 'blob_source_test.dart' as blob_source;

void main() {
  string_source.main();
  blob_source.main();
}