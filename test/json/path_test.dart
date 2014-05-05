library json.path_test;

import 'package:unittest/unittest.dart';
import '../../lib/json/path.dart';

final TEST_MAP = 
    { 'items' : [
        { 'title' : 'Stuff for stuffing',
          'meta' : {
            'author' : 'Ann Author',
            'publisher' : 'Some publisher',
            'pagemap' : {
              'page_one' : 1,
              'page_two' : 2
            }
          }
        },
        { 'title' : 'Things for thinging',
          'meta' : {
            'author' : 'Ann Other Author',
            'publisher' : 'Associated publisher',
            'pagemap' : {
              'page_one' : 1,
              'page_two' : 2
              
            }
          }
        }
      ]
    };

void main() {
  group("paths", () {
    test("should be able to parse and read a path", () {
      var path = new FieldPath(
                'page_one',
                parent: new FieldPath(
                    'pagemap', 
                    parent: new FieldPath(
                        'meta', 
                        parent: new IndexPath(0, parent: new FieldPath('items')))));
      var rawPath = 'items[0].meta.pagemap.page_one';
      expect(Path.parse(rawPath), path);
    });
    
    test("should be able to get the value corresponding to 'items[1].title'", () {
      var path = new FieldPath(
          'title',
          parent: new IndexPath(1, parent: new FieldPath('items')));
      expect(path.getValue(TEST_MAP), 'Things for thinging');
    });
    test("should be able to get the value corresponding to 'items[0].meta.pagemap.page_one'", () {
      var path = Path.parse('items[0].meta.pagemap.page_one');
      expect(path.getValue(TEST_MAP), 1, reason: "before set value");
      path.setValue(TEST_MAP, 15);
      expect(path.getValue(TEST_MAP), 15, reason: "after set value");
    });
  });
  
}