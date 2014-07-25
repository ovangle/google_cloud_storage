library json.token_test;

import 'dart:async';
import 'package:unittest/unittest.dart';

import '../../lib/connection/resume_token.dart';


void main() {
  group("ResumeToken", () {
    test("should store type, uploadUri and selector", () {
      Completer c = new Completer();
      ResumeToken token = new ResumeToken(Uri.parse("http://www.example.com"), selector: '*',
          completer: c);
      expect(token.toJson(), {'uploadUri': 'http://www.example.com', 'selector': '*'});
    });

    test("should throw when accessing done future on a deserialized token", () {
      ResumeToken token = new ResumeToken.fromJson({'uploadUri': 'http://www.example.com', 'selector': '*'});
      expect(() => token.done, throws);
    });
  });
}