library json.token_test;

import 'dart:async';
import 'package:http/http.dart';
import 'package:unittest/unittest.dart';

import '../../lib/connection/resume_token.dart';
import '../../lib/api/api.dart';
import '../../lib/connection/rpc.dart';

void main() {
  group("ResumeToken", () {
    test("should store type, uploadUri and selector", () {
      Completer c = new Completer();
      ResumeToken token = new ResumeToken(Uri.parse("http://www.example.com"), selector: '*',
          done: c.future);
      expect(token.toJson(), {'uploadUri': 'http://www.example.com', 'selector': '*'});
    });

    test("should throw when accessing done future on a deserialized token", () {
      ResumeToken token = new ResumeToken.fromJson({'uploadUri': 'http://www.example.com', 'selector': '*'});
      expect(() => token.done, throws);
    });

    test("should return StorageObject when RpcResponse is received", () {
      Completer c = new Completer();
      String mockResponse = """
      {
       \"name\": \"alphabet_soup2\",
       \"bucket\": \"ovangle-test\"
      }
      """;
      ResumeToken token = new ResumeToken(Uri.parse("www.google.com"), selector: '*', done: c.future);

      token.done.then(expectAsync((result) => expect(result, new isInstanceOf<StorageObject>())));
      c.complete(new RpcResponse(new Response(mockResponse, 200, headers: {'content-type': 'application/json'})));
    });
  });
}