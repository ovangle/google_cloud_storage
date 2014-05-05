library json.selector_test;

import 'package:unittest/unittest.dart';

import '../../lib/json/path.dart';
import '../../lib/json/selector.dart';

final TEST_MAP = 
  { 'kind' : 'demo',
    'items' : 
    [ 
      { 
        'title' : 'First Title',
        'author' : 'An author',
        'characteristics' : {
          'length' : 'short',
          'accuracy' : 'high',
          'followers' : ['Jo', 'Will']
        },
        'year' : 1956
      },
      {
        'title' : 'Second Title',
        'author' : 'Another author',
        'characteristics' : null,
        'year' : 1957
      }
    ],
    'etag' : 'some_etag',
    'context' : 
    {
      'facets' :
      [ {'label' : 'a label' },
        {'label' : 'another label'}
      ],
      'pagemap' : 
      {
        'page_1' : { 
          'title' : 'First page',
          'page_number' : 1
        },
        'page_2' : { 
          'title' : 'Second page',
          'page_number' : 2
        }
      }
    }
  };

void main() {
  group("field selector", () {
    group("'items'", () {
      var selector = new FieldSelector('items');
      test("read and show", () {
        expect(selector.toString(), 'items');
        expect(Selector.parse('items'), selector);
      });
      test("should select all elements in the 'items' array", () {
        expect(
            selector.select(TEST_MAP),
            { 'items' : 
              [ 
                { 
                  'title' : 'First Title', 
                  'author' : 'An author',
                  'characteristics' : {
                    'length' : 'short',
                    'accuracy' : 'high',
                    'followers' : ['Jo', 'Will']
                  },
                  'year' : 1956
                },
                { 'title' : 'Second Title', 
                  'author' : 'Another author',
                  'characteristics' : null,
                  'year' : 1957
                }
              ]
            }
        );
      });
      test("path 'items' should be in selector", () {
        var items = new IndexPath(0, parent: new FieldPath('items'));
        expect(selector.isPathInSelection(items), true);
      });
      test("path 'items/title' should be in selector", () {
        var s = new FieldPath('title', parent: new IndexPath(0, parent: new FieldPath('items')));
        expect(selector.isPathInSelection(s), true);
      });
      test("path 'context' should not be in selector", () {
        var s = new FieldPath('context');
        expect(selector.isPathInSelection(s), false);
      });
    });
    
    group("'items/title'", () {
      var selector = new FieldSelector("title", parent: new FieldSelector('items'));
      test("read and show", () {
        expect(selector.toString(), 'items/title');
        expect(Selector.parse('items/title'), selector);
      });
      test("should select only the title field from `items`", () {
        expect(selector.select(TEST_MAP), 
            {'items' :
              [ { 'title' : 'First Title' },
                { 'title' : 'Second Title' }
              ]
            });
      });
      test("path 'items[0].title' should be in selection", () {
        var s = new FieldPath('title', parent: new IndexPath(0, parent: new FieldPath('items')));
        expect(selector.isPathInSelection(s), true);
      });
      test("path 'items[0].characteristics should not be in selector", () {
        var s = new FieldPath('characteristics', parent: new IndexPath(0, parent: new FieldPath('items')));
        expect(selector.isPathInSelection(s), false);
      });
    });
    group("'items/characteristics/length'", () {
      var selector = new FieldSelector(
          'length',
          parent: new FieldSelector('characteristics', parent: new FieldSelector('items')));
      test("read and show", () {
        expect(selector.toString(), 'items/characteristics/length', reason: 'show');
        expect(Selector.parse('items/characteristics/length'), selector, reason: 'read');
      });
      test("should select only length from the populated charactersistics of the items array", () {
        expect(selector.select(TEST_MAP), 
            { 'items':
              [ 
               { 'characteristics': { 'length' : 'short' } },
               { 'characteristics': null }
              ]
            });
      });
    });
    group("'context/facets/label'", () {
      var selector = new FieldSelector(
          'label', 
          parent: new FieldSelector(
              'facets', 
              parent: new FieldSelector('context')));
      test("read and show", () {
        expect(selector.toString(), "context/facets/label");
        expect(Selector.parse('context/facets/label'), selector);
      });
      test("should select only the label field from the facets array in the context object", () {
        expect(selector.select(TEST_MAP), 
            { 'context':
              { 'facets' : 
                [
                  {'label' : 'a label'}, 
                  {'label' : 'another label'}
                ]
              }
            }
        );
      });
    });
  });
  group("any selector", () {
    group("'context/pagemap/*/title", () {
      var selector = new FieldSelector(
          "title",
          parent: new AnySelector(
              parent: new FieldSelector(
                  "pagemap",
                  parent: new FieldSelector("context"))));
      test("read and show", () {
        expect(selector.toString(), 'context/pagemap/*/title', reason: "show");
        expect(Selector.parse('context/pagemap/*/title'), selector, reason: "read");
      });
      test("should select all titles from the entries in pagemap", () {
        expect(selector.select(TEST_MAP), 
            { 'context' : 
              {
                'pagemap' : 
                { 'page_1' : { 'title' : 'First page' },
                  'page_2' : { 'title' : 'Second page' }
                }
              }
            });
      });
      test("path 'context.pagemap.page_1.title' should be in selection", () {
        var p = new FieldPath(
            'title', 
            parent: new FieldPath(
                'page_1',
                parent: new FieldPath(
                    'pagemap',
                    parent: new FieldPath('context'))));
        expect(selector.isPathInSelection(p), true);
      });
      test("path 'context/pagemap/page_2/page_number' shold not be in selection", () {
        var p = new FieldPath(
                    'page_number', 
                    parent: new FieldPath(
                        'page_2',
                        parent: new FieldPath(
                            'pagemap',
                            parent: new FieldPath('context'))));
        expect(selector.isPathInSelection(p), false);
      });
    });
  });
  group("sublist selector", () {
    group("'etag,items'", () {
      var selector = new FieldListSelector(
          [ new FieldSelector('etag'), 
            new FieldSelector('items')
          ]);
      test("read and show", () {
        expect(selector.toString(), 'etag,items');
        expect(Selector.parse('etag,items'), selector);
      });
      
      test("should select the etag field and items arrrays", () {
        expect(
            selector.select(TEST_MAP), 
            { 'items' : 
              [ 
                { 'title' : 'First Title', 
                  'author' : 'An author',
                  'characteristics' : {
                    'length' : 'short',
                    'accuracy' : 'high',
                    'followers' : ['Jo', 'Will']
                  },
                  'year' : 1956
                },
                { 
                  'title' : 'Second Title', 
                  'author' : 'Another author',
                  'characteristics' : null,
                  'year' : 1957
                }
              ],
              'etag' : 'some_etag',
            }
        );  
      });
      test("path 'etag' should be in selector", () {
        var s = new FieldPath('etag');
        expect(selector.isPathInSelection(s), true);
      });
      test("path 'context.facets' should not be in selector", () {
        var s = new FieldPath('facets', parent: new FieldPath('context'));
        expect(selector.isPathInSelection(s), false);
      });
    });
    group("'items (title, characteristics/followers, author)'", () {
      var selector = new FieldListSelector(
          [ new FieldSelector('title'), 
            new FieldSelector('followers', parent: new FieldSelector('characteristics')),
            new FieldSelector('author'),
          ], 
          parent: new FieldSelector('items')
      );
      test("read and show", () {
        expect( 
            selector.toString(), 
            "items(title,characteristics/followers,author)", 
            reason:'show');
        expect(Selector.parse('items (title, characteristics/followers, author)'),
            selector, reason: 'read');
      });
      test("should select title, characteristics/followers and author from items field", () {
        expect(
            selector.select(TEST_MAP), 
            { 'items' :
              [
                {
                  'title' : 'First Title',
                  'characteristics' : { 'followers' : ['Jo', 'Will'] },
                  'author' : 'An author'
                },
                {
                  'title' : 'Second Title',
                  'characteristics' : null,
                  'author' : 'Another author' 
                }
              ]
            }
        );
       
      });
      test("path 'items[1].characteristics.followers' should be in selection", () {
        var s = new FieldPath(
            'followers',
            parent: new FieldPath(
                'characteristics',
                parent: new IndexPath(
                    1, 
                    parent: new FieldPath('items'))));
        expect(selector.isPathInSelection(s), true);
      });
      test("path 'items[0].year' should not be in selection", () {
        var s = new FieldPath('year', parent: new IndexPath(0, parent: new FieldPath('items')));
        expect(selector.isPathInSelection(s), false);
      });
    });
  });
}