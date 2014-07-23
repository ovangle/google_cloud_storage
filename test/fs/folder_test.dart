library folder_test;

import 'dart:async';

import 'package:unittest/unittest.dart';

import 'package:google_cloud_storage/fs/fs.dart';
import 'package:google_cloud_storage/testing/testing_common.dart';

void main() {
  group("folder", () {
    var filesystem;

    setUp(() {
      var connection = new MockConnection();
      return connection.createBucket('fs-bucket').then((bucket) {
        filesystem = new Filesystem(
            connection,
            'fs-bucket');
      });
    });

    group('paths', () {
      test("root should be a valid folder", () {
        var root = new RemoteFolder(filesystem, '/');
        expect(root.path, '/');
      });

      test("Should be able to get the parent of a folder", () {
        var f = new RemoteFolder(filesystem, "/folder1/subfolder1/");
        expect(f.parent, new RemoteFolder(filesystem, "/folder1/"));
      });
    });

    group('', () {
      setUp(() {
        var folder1 = new RemoteFolder(filesystem, '/folder1/');
        var folder1_sub1 = new RemoteFolder(filesystem, '/folder1/subfolder1/');
        var folder1_sub2 = new RemoteFolder(filesystem, '/folder1/subfolder2/');

        return folder1.create().then((_) {
          return Future.wait([
            folder1_sub1.create(),
            folder1_sub2.create()
          ]);
        });
      });

      test("should be able to test for a folder's existence", () {
        var folder = new RemoteFolder(filesystem, "/folder1/");
        return folder.exists().then((response) {
          expect(response, true);
        });
      });

      test("should be able to list folders at top level", () {
        return filesystem.root.list().toList().then((folders) {
          expect(folders, [new RemoteFolder(filesystem, '/folder1/')]);
        });
      });

      test("should be able to list the subfolders of a folder", () {
        var folder = new RemoteFolder(filesystem, '/folder1/');
        return folder.list().toList().then((folders) {
          expect(folders, [
            new RemoteFolder(filesystem, '/folder1/subfolder1/'),
            new RemoteFolder(filesystem, '/folder1/subfolder2/')
          ]);
        });
      });
    });
  });
}