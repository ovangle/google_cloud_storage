library json.token_test;

import 'dart:async';
import 'package:unittest/unittest.dart';

import '../../lib/connection/resume_token.dart';

void main() {
  group("ResumeToken", () {
    test("should store type, uploadUri and selector", () {
      Completer c = new Completer();
      ResumeToken token = new ResumeToken(ResumeToken.TOKEN_INIT, Uri.parse("http://www.example.com"), selector: '*',
          done: c.future);
      expect(token.toJson(), {'type': 0, 'uploadUri': 'http://www.example.com', 'selector': '*'});
    });
  });
}