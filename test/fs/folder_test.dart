library folder_test;

import 'package:unittest/unittest.dart';

import '../../lib/fs/fs.dart';
import '../mock_connection.dart';

final connection = new MockConnection();
final fs = new CloudFilesystem(connection, 'fs_bucket');

void main() {
  group("folder", () {
    tearDown() {
      connection.clearData();
    }

    test("Should be able to get the parent of a folder", () {
      var f = new Folder(fs, "folder1/subfolder1/");
      expect(f.parent, new Folder(fs, "folder1/"));
    });

    test("should be able to test for existence", () {
      var folder = new Folder(fs, "folder1");
      return folder.exists().then((response) {
        expect(response, false);
        connection.objects
            .add({'bucket': 'fs_bucket', 'name': 'folder1/'});
        return folder.exists().then((response) {
          expect(response, true);
        });
      });
    });

    test("should be able to list the contents of a folder", () {
      connection.objects
          ..addAll([
              {'bucket': 'fs_bucket', 'name': 'folder1/'},
              {'bucket': 'fs_bucket', 'name': 'folder1/subfolder1/file1'},
              {'bucket': 'fs_bucket', 'name': 'folder2/'},
              {'bucket': 'fs_bucket', 'name': 'folder1/file1', 'contentType': 'text/plain'}
          ]);
      var folder = new Folder(fs, "folder1");
      return folder.list().toList()
      .then((contents) {
        print(contents);
        expect(contents,
            unorderedEquals([
                new Folder(fs, 'folder1/subfolder1/'),
                new RemoteFile(fs, 'folder1/file1', 'text/plain')
            ])
        );
      });
    });
  });
}