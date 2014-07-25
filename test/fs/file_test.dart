library file_test;


import 'package:unittest/unittest.dart';
import 'package:google_cloud_storage/testing/testing_common.dart';
import 'package:google_cloud_storage/fs/fs.dart';
import 'package:google_cloud_storage/connection/rpc.dart';

void main() {
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

  group('copy', () {
    test("It should be possible to copy a file", () {
      var source = new RemoteFile(filesystem, '/source.jpg');
      return source.write(new StringSource('foo', 'text/plain'))
          .then((RemoteFile file) {
            var destination = new RemoteFile(filesystem, '/parent/destination.jpg');
            return source.copyTo(destination);
          })
          .then((RemoteFile file) {
            expect(file.parent.name, 'parent/');
            expect(file.name, 'destination.jpg');
          });
    });
    test("Copy should fail if the destination file is not existing", () {
      var source = new RemoteFile(filesystem, '/source.jpg');
      var destination = new RemoteFile(filesystem, '/destination.jpg');
      return source.copyTo(destination)
          .then((RemoteFile file) {
            expect(true, isFalse);
          })
          .catchError((error) {
            expect(error is RpcException, isTrue);
          });
    });
  });
}