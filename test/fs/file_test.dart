library file_test;


import 'package:unittest/unittest.dart';
import 'package:google_cloud_storage/testing.dart';
import 'package:google_cloud_storage/fs.dart';

void main() {
  var filesystem;

  setUp(() {
    var connection = new MockConnection();
    return connection.createBucket('fs-bucket').then((bucket) {
      filesystem = new CloudFilesystem(
          connection,
          'fs-bucket');
    });
  });

  group('paths', () {
    test("A file in root shoudl be valid", () {
      var master_jpg = new RemoteFile(filesystem, '/master.jpg');
      expect(master_jpg.path, '/master.jpg');
    });

    test("The parent of a file should be the containing folder", () {
      var f = new RemoteFile(filesystem, "/folder1/file1.txt");
      expect(f.parent, new RemoteFolder(filesystem, "/folder1/"));
    });

    test("The name of the file should be the path relative to its parent", () {
      var f = new RemoteFile(filesystem, '/folder1/file1.txt');
      expect(f.name, 'file1.txt');
    });
  });
}