library json.all_tests;

import 'package:unittest/unittest.dart';

import 'object_test.dart' as object;
import 'path_test.dart' as path;
import 'selector_test.dart' as selector;
import 'token_test.dart' as token;

void main() {
  group("json", () {
    object.main();
    path.main();
    selector.main();
    token.main();
  });
}