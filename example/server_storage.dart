/**
 * Demonstrates how to use the `google_cloud_storage` dart library to transfer files
 * between a server side application and the
 */
library example.server_storage;

import 'dart:io';
import 'package:logging/logging.dart';

import 'package:google_cloud_storage/cloud_storage_server.dart';

final projectNumber =  '451906218297';
final projectId = 'crucial-matter-487';

final  serviceAccount ='451906218297-2ergc59ejm6i8i4qmkto2otb431215ji@developer.gserviceaccount.com';
final pathToPrivateKey =  '/Users/ovangle/Programming/auth/exitlive2.pem';

CloudStorageConnection connection;

void main() {
  Logger.root.onRecord.listen(print);
  CloudStorageConnection.open(
      projectNumber, projectId,
      serviceAccount: serviceAccount,
      pathToPrivateKey: pathToPrivateKey,
      role: PermissionRole.OWNER
  ).then((conn) {
    connection = conn;
  }).then((_) {
    //connection.getBucket('ovangle-test', queryParams: {'projection': 'full'}).then((bucket)
    //uploadFile();
  }).then((_) {
    //downloadFile();
  }).then((_) {
    renameFile();
  });
}

void uploadFile() {
  var file = new FileSource(new File('test1.mp3'), 'audio/mpeg');
  var object = new StorageObject('ovangle-test', 'bin%20test1.mkv');

  connection.uploadObject('ovangle-test', object, file).then((resumeToken) {
    return resumeToken.done.then((obj) {
      print(obj);
    }).catchError((err) {
      print(err);
    });
  });

}

void downloadFile() {
  connection.downloadObject('ovangle-test', 'bin%20test1.mkv').listen((bytes) {
    print("Received: $bytes");
  });
}

void renameFile() {
  connection.patchObject(
      'ovangle-test',
      'alphabet_soup2',
      (obj) {
        obj.contentLanguage = 'en';
      },
      queryParams: {'fields': 'bucket,name,contentLanguage'}
  ).then((obj) {
    print(obj);
  });
}
