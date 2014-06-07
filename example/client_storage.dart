
import 'dart:async';
import 'dart:html' as dom;

import 'package:google_oauth2_client/google_oauth2_browser.dart' as oauth2;
import 'package:google_cloud_storage/cloud_storage_client.dart';

const String PROJECT_ID = "crucial-matter-487";
const String CLIENT_ID = "451906218297-o5lg4drf2oun3b4odthb05l9g9aoarb6.apps.googleusercontent.com";

oauth2.GoogleOAuth2 context;

void main() {
  context = new oauth2.GoogleOAuth2(
        CLIENT_ID,
        [ 'https://www.googleapis.com/auth/userinfo.email',
          'https://www.googleapis.com/auth/devstorage.full_control'
        ],
        approval_prompt: null,
        autoLogin: true);
  context.login(immediate:true)
      .catchError((err) {
    var button = dom.document.createElement('button');
    var body = dom.querySelector('body');
    body.append(button);
    button.onClick.listen((_) {
      context.login();
    });
    return true;
  })
  .whenComplete(() {
    dom.InputElement fileInput = dom.querySelector("#file-input");
    fileInput.onChange.listen((evt) {
      var files = fileInput.files;
      if (files.isNotEmpty) {
        var file = files.first;
        uploadFile(file).then((file) {
          print("uploaded");
        });
      }
    });
  });
}

Future uploadFile(dom.File file) {
  var connection = getConnection();

  //Need to create a bucket to store the files
  return ensureAccessToBucket('ovangle-test')
      .then((_) {
        return connection.uploadObject(
          "ovangle-test",
          "file50",
          "audio/mp3",
          new BlobSource(file));
      });
}

Future<StorageBucket> ensureAccessToBucket(String name) {
  var connection = getConnection();
  return connection.getBucket(name, selector: 'name,cors').then((bucket) {
    return connection.updateBucket(name, (b) {
        b.cors.clear();
        b.cors.add(
            new CorsConfiguration()
                ..maxAgeSeconds = 5000
                ..method.addAll(['GET', 'HEAD', 'PUT', 'POST', 'OPTIONS'])
                ..origin.addAll(['*'])
        );
      }).then((bucket) {
        print('cors: ${bucket.cors}');
        return bucket;
      });
    return bucket;
  }).catchError((err) {
      //The bucket doesn't exist. Create it.
      StorageBucket bucket = new StorageBucket(name);
      bucket.cors.add(
          new CorsConfiguration()
            ..maxAgeSeconds = 5000
            ..method.addAll(['GET', 'HEAD', 'PUT', 'POST'])
            ..origin.addAll(['*'])
      );
      return bucket;

  }, test: (err) => err is RPCException && err.statusCode == 404);


}

CloudStorageConnection getConnection() {
  return new CloudStorageConnection(
      PROJECT_ID,
      context
  );
}