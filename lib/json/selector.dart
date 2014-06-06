library google_cloud_storage.json.selector;

import 'package:collection/wrappers.dart';
import 'package:collection/equality.dart';
import 'package:quiver/core.dart' as qcore;

import 'path.dart';


const _LIST_EQ = const ListEquality();

abstract class Selector {
  /**
   * Entry point for the parser is an (unparenthesised) selector list.
   */
  static Selector parse(String input) {
    _Tokenizer tokenizer = new _Tokenizer(input);
    return FieldListSelector._parseSelectorList(tokenizer);
  }

  /**
   * Parses a selector fragment with `BNF`
   *     selector := any_selector | field_selector
   */
  static Selector _parse(_Tokenizer tokenizer) {
    if (tokenizer.current == null)
      tokenizer.moveNext();
    switch (tokenizer.current.type) {
      case _Token.ASTERISK:
        return AnySelector._parse(tokenizer);
      case _Token.FIELD_NAME:
        return FieldSelector._parse(tokenizer);
      default:
        throw new FormatException("Invalid character in selector at ${tokenizer.index}");
    }
  }

  /**
   * Parses a selector fragment with `BNF`
   *     subselector := ('/' selector | subselection_list)?
   *
   * Returns `null` if there was no match.
   */
  static Selector _parseSubselector(tokenizer) {
    assert(tokenizer.current != null);
    switch (tokenizer.current.type) {
      case _Token.SLASH:
        if (!tokenizer.moveNext())
          throw new FormatException('Expected subselector at ${tokenizer.index}');
        return _parse(tokenizer);
      case _Token.L_PARENS:
        return FieldListSelector._parseSubselections(tokenizer);
      default:
        return null;
    }
  }

  Selector parent;

  Selector(this.parent);

  Selector get root => parent == null ? this : parent.root;

  List<Selector> get _ancestors =>
      ((parent == null ? [] : parent._ancestors) as List)
      ..add(this);

  /**
   * Check whether the provided `path` is in the selection defined by `this`.
   *
   */
  bool isPathInSelection(Path path) {
    path = _firstNonIndexAncestor(path);
    if (path == null) {
      //A path containing only indexes only matches against an `AnySelector`
      return (this is AnySelector && parent == null);
    }
    return _matchPath(path, matchSubpaths: true);
  }

  /**
   * Test whether the field `name` would be included in the filtered
   * output of the selector.
   */
  bool _matchPath(Path path, {bool matchSubpaths, int matchDepth});
  Map<String,dynamic> _filterMap(Map<String,dynamic> map);

  /**
   * Filters values from the map so that only values which match
   * the [Selector] are included in the map.
   *
   * The structure of the map is preserved by a [:select:] operation.
   */
  Map<String,dynamic> select(Map<String,dynamic> map) {
    //The current selector is the finest selector, but filtered selectors
    //need to be applied from coarsest to finest.
    //Build a stack of filters to be applied in order
    return root._select(map, _ancestors.reversed.toList());
  }

  /**
   * Get the inner selection (which doesn't include values outside the
   * scope of the parent selector).
   *
   * If [:inner:] is a [Map], returns the selection of inner from the child
   * selectors.
   * If [:inner:] is a [List], applies the child selectors to each value in
   * the list. If
   */
  _selectInner(dynamic inner, List<Selector> childSelectors) {
    var selection;

    if (inner is Map) {
      //Just recurse on the map.
      selection = _select(inner, childSelectors);
    } else if (inner is List) {
      //Consturct a list of the items for which a valid selection was
      //returned by the child selectors
      selection = new List();
      for (var item in inner) {
        if (item is Map) {
          //If item is a map, select it.
          var selectedItem = _select(item, childSelectors);
          selection.add(selectedItem);
        } else {
          //Add the value unaltered to the list.
          selection.add(item);
        }
      }
    } else {
      //If there is no more selections to do, return the value.
      if (childSelectors.isEmpty) {
        selection = inner;
      }
    }
    return selection;
  }

  /**
   * The workhorse method of filter selection.
   *
   * [:childSelectors:] is a list of child selectors, ordered from finest to coarsest
   * (where a selector is more coarse if it selects keys further from the root.
   */
  Map<String,dynamic> _select(Map<String,dynamic> selectFrom, List<Selector> childSelectors) {
    if (childSelectors.isEmpty)
      return selectFrom;
    var childSelector = childSelectors.removeLast();
    var filtered = childSelector._filterMap(selectFrom);
    var selected = new Map.fromIterable(
        filtered.keys,
        value: (k) => childSelector._selectInner(filtered[k], childSelectors)
    );
    childSelectors.add(childSelector);
    return selected;
  }
}

class AnySelector extends Selector {
  /**
   * Parses a selector fragment with `BNF`
   *      any_selector := '*' subselector?
   *
   * Returns the finest selector parsed.
   */
  static Selector _parse(_Tokenizer tokenizer) {
    assert(tokenizer.current.type == _Token.ASTERISK);
    var s = new AnySelector();
    if (!tokenizer.moveNext())
      return s;
    Selector subselector = Selector._parseSubselector(tokenizer);
    if (subselector != null) {
      subselector.parent = s;
      return subselector;
    }
    return s;
  }

  AnySelector({Selector parent}) : super(parent);

  bool _matchPath(FieldPath path, {matchSubpaths: false, matchDepth: 0}) {

    if (parent == null) {
      // match '*' against path
      return matchSubpaths;
    }
    if (path.parent == null) {
      // match 'items/*' against 'something'
      return path.fieldDepth == matchDepth;
    }
    path = _firstNonIndexAncestor(path.parent);
    return parent._matchPath(path, matchSubpaths: false, matchDepth: matchDepth);
  }

  Map<String,dynamic> _filterMap(Map<String,dynamic> map) => new Map.from(map);

  String toString() => (parent == null) ? '*' : '$parent/*';

  bool operator ==(Object other) => other is AnySelector;
  int get hashCode => 0;
}

class FieldSelector extends Selector {
  /**
   * Parses a selector fragment with the BNF
   *     field_name := [a-zA-Z_]+
   *     field_selector := field_name subselector?
   *
   * Returns the finest selector parsed (with the parent of the selector set).
   */
  static Selector _parse(_Tokenizer tokenizer) {
    assert(tokenizer.current.type == _Token.FIELD_NAME);
    var field = new FieldSelector(tokenizer.current.content);
    if (!tokenizer.moveNext()) {
      return field;
    }
    Selector subselector = Selector._parseSubselector(tokenizer);
    if (subselector != null) {
      subselector.root.parent = field;
      return subselector;
    }
    return field;
  }


  final String name;
  FieldSelector(this.name, {Selector parent}) : super(parent);

  /**
   * matches the path against `this`.
   *
   * If [:matchSubpaths:] is `true`, then subpaths will be allowed in the match
   * (so `items.characteristics.followers` will match against `items`)
   * [:matchDepth:] is the depth of the selector which the match occurs.
   *
   */
  bool _matchPath(FieldPath path, {bool matchSubpaths: true, int matchDepth: 0}) {
    if (matchSubpaths) {
      while (path.name != name) {
        //Ignore index paths
        path = _firstNonIndexAncestor(path.parent);
        if (path == null) return false;
      }
    } else {
      if (path.name != name)
        return false;
    }
    if (parent == null) {
      //Ignore IndexPath
      path = _firstNonIndexAncestor(path);
      //Match the path at the specified matchDepth.
      return path.fieldDepth == matchDepth;
    } else {
      //Skip indexpaths
      path = _firstNonIndexAncestor(path.parent);
      if (path.parent == null) {
        //This is a match like 'items/characteristics' on 'items'
        return true;
      }
      return parent._matchPath(path, matchSubpaths: false, matchDepth: matchDepth);
    }
  }

  /**
   * Apply the selector directly to the map.
   */
  dynamic _filterMap(Map<String,dynamic> map) => {name: map[name]};

  bool operator ==(Object other) =>
      other is FieldSelector &&
      other.parent == parent &&
      other.name == name;

  int get hashCode => qcore.hash2(parent, name);
  String toString() => (parent != null) ? "$parent/$name" : name;
}

/**
 * Represents the union of selecting a number of fields from a `JSON` map.
 */
class FieldListSelector extends Selector {
  /**
   * Parses a selector fragment with the BNF
   *      subselection_list = '(' selector_list ')'
   */
  static Selector _parseSubselections(_Tokenizer tokenizer) {
    assert(tokenizer.current.type == _Token.L_PARENS);
    if (!tokenizer.moveNext()) {
      throw new FormatException('Unexpected end of selector');
    }
    Selector subselections = _parseSelectorList(tokenizer);
    if (tokenizer.current == null || tokenizer.current.type != _Token.R_PARENS) {
      throw new FormatException('Expected end of subselector list at ${tokenizer.index}\n');
    }
    tokenizer.moveNext();
    return subselections;
  }

  /**
   * Parses a selector fragment with the BNF
   *      selector_list := selector selector_list_tail?
   */
  static Selector _parseSelectorList(_Tokenizer tokenizer) {
    List<Selector> selectors = [Selector._parse(tokenizer)];
    _parseListTail(tokenizer, selectors);
    if (selectors.length == 1) {
      return selectors[0];
    }
    return new FieldListSelector(selectors);
  }

  /**
   * Parses a selector fragment with the BNF
   *      selector_list_tail := (',' selector)* (')')?
   */
  static void _parseListTail(_Tokenizer tokenizer, List<Selector> selectors) {
    if (tokenizer.current == null)
      return;
    switch (tokenizer.current.type) {
      case _Token.COMMA:
        if (!tokenizer.moveNext())
          throw new FormatException('Unexpected end of selector');
        selectors.add(Selector._parse(tokenizer));
        _parseListTail(tokenizer, selectors);
        return;
      case _Token.R_PARENS:
        return;
      default:
        throw new FormatException("Invalid character in selector at ${tokenizer.index}");
    }
  }

  final bool isPath = false;

  final UnmodifiableListView<FieldSelector> fields;
  FieldListSelector(List<FieldSelector> fields, {Selector parent}) :
    super(parent),
    this.fields = new UnmodifiableListView(fields);

  bool _matchPath(FieldPath path, {matchSubpaths: true, matchDepth: 0}) {
    var p = parent;
    while (p != null) {
      matchDepth++;
      p = p.parent;
    }
    if (path.fieldDepth < matchDepth) {
      // A match of 'items' against 'items (characteristics, followers)'
      return parent._matchPath(path, matchSubpaths: false);
    }
    bool matchesField = fields.any(
        (f) => f._matchPath(path, matchSubpaths: matchSubpaths, matchDepth: matchDepth));
    if (!matchesField) return false;
    if (parent == null) {
      // Match 'items,etag' against 'items/characteristics'
      return matchSubpaths;
    }
    //Allow subpaths in the match, since we've already matched at the correct depth
    //against one of the fields.
    return parent._matchPath(path, matchSubpaths: true);
  }

  Map<String,dynamic> _filterMap(Map<String,dynamic> map) =>
      new Map.fromIterable(
          fields,
          key: (k) => k.root.name,
          value: (k) {
            var fieldValue = map[k.root.name];
            if (fieldValue is Map) {
              return k.select(map)[k.root.name];
            }
            return fieldValue;
          });

  bool operator ==(Object other) {
    if (other is FieldListSelector) {
      if (parent != other.parent) return false;
      if (fields.length != other.fields.length)return false;
      return _LIST_EQ.equals(fields, other.fields);
    }
    return false;
  }

  int get hashCode => qcore.hash2(parent, _LIST_EQ.hash(fields));

  String toString() {
    String listContent = fields.join(',');
    if (parent == null)
      return listContent;
    return "$parent($listContent)";
  }
}

/**
 * The first parent of [path], ignoring any [IndexPath]s
 */
FieldPath _firstNonIndexAncestor(Path path) {
  while (path is IndexPath)
    path = path.parent;
  return path;
}

class _Tokenizer implements Iterator<_Token> {
  static final _FIELD_NAME_PATTERN = new RegExp(r'[a-zA-Z][a-zA-Z0-9_]*');
  static final _WHITESPACE_PATTERN = new RegExp(r'\s+');

  final String input;
  int index;
  _Token _current;

  _Tokenizer(this.input):
    index = 0;

  _Token get current => _current;

  bool moveNext() {
    if (_current != null) {
      index += _current.length;
    }
    _current = null;

    _skipWhitespace();

    if (index >= input.length)
      return false;

    var match = _FIELD_NAME_PATTERN.matchAsPrefix(input, index);
    if (match != null)
      _current = new _Token(_Token.FIELD_NAME, match.group(0));
    if (input.startsWith('(', index))
      _current = new _Token(_Token.L_PARENS, '(');
    if (input.startsWith(')', index))
      _current = new _Token(_Token.R_PARENS, ')');
    if (input.startsWith(',', index))
      _current = new _Token(_Token.COMMA, ',');
    if (input.startsWith('/', index))
      _current = new _Token(_Token.SLASH, '/');
    if (input.startsWith('*', index))
      _current = new _Token(_Token.ASTERISK, '*');

    if (_current == null) {
      throw new FormatException('Invalid character in stream at $index');
    }
    return true;
  }

  void _skipWhitespace() {
    var wspace = _WHITESPACE_PATTERN.matchAsPrefix(input, index);
    if (wspace != null)
      index += wspace.group(0).length;
  }
}

class _Token {
  static const FIELD_NAME = 0;
  static const L_PARENS = 1;
  static const R_PARENS = 2;
  static const COMMA = 3;
  static const SLASH = 4;
  static const ASTERISK = 5;


  /**
   * The type of the token.
   */
  final int type;
  /**
   * The content of the token.
   */
  final String content;

  int get length => content.length;

  _Token(this.type, this.content);
}

