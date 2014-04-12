/**
 * Paths into json maps.
 */
library google_file_storage.json.path;

import 'error.dart';

abstract class Path {
  static Path parse(String rawPath) {
    return parseField(new _Tokenizer(rawPath) ..moveNext());
  }
  
  Path _parent;
  Path get parent => _parent;
  
  Path get root => parent == null ? this : parent.root;
  
  Path(this._parent);
  
  /**
   * Build a new path from the 
   */
  factory Path.fromPath(Path path, {Path pathToRoot}) {
    if (path == null) return pathToRoot;
    
    var parent = new Path.fromPath(path.parent, pathToRoot: pathToRoot);
    if (path is FieldPath) {
      return new FieldPath(path.name, parent: parent);
    } else if (path is IndexPath) {
      return new IndexPath(path.index, parent: parent);
    }
    throw new UnsupportedError("Unrecognised path type: ${path.runtimeType}");
  }
  
  List<Path> get pathElements => 
      (parent == null ? [] : parent.pathElements)..add(this);
 
  dynamic getValue(var json);
  void setValue(var json, dynamic value);
}

class FieldPath extends Path {
  final String name;
  
  FieldPath(this.name, {Path parent}) : super(parent);
  
  dynamic getValue(var json) {
    if (parent != null) {
      json = parent.getValue(json);
    }
    if (json is Map) {
      return json[name];
    }
    throw new PathError.expectedMap(json);
  }
  
  /**
   * Set the value at the path in the json map.
   */
  void setValue(var json, dynamic value) {
    if (parent != null) {
      json = parent.getValue(json);
    }
    if (json is Map) {
      json[name] = value;
      return;
    }
    throw new PathError.expectedMap(json);
  }
  
  String toString() => 
      parent == null ? name : "$parent.$name";
  
  bool operator ==(Object other) =>
      other is FieldPath &&
      other.name == name &&
      other.parent == parent;
  
  int get hashCode => 17 * parent.hashCode + name.hashCode;
}

/**
 * An index into a json list.
 */
class IndexPath extends Path {

  int index;
  
  IndexPath(this.index, {Path parent}) : super(parent);
  
  getValue(var json) {
    if (parent != null)
      json = parent.getValue(json);
    if (json is List)
      return json[index];
    throw new PathError.expectedList(json);
  }
  
  /**
   * Set the value of the path in the json map. 
   * A [JsonError] is raised if the value is not a simple type.
   */
  void setValue(var json, dynamic value) {
    if (parent != null) {
      json = parent.getValue(value);
    }
    _checkSimpleType(value);
    if (json is List) {
      json[index] = value;
      return;
    }
    throw new PathError.expectedList(value);
  }
  
  String toString() =>
    parent == null ? "[$index]" : "$parent[$index]";
  
  bool operator ==(Object other) =>
      other is IndexPath &&
      other.index == index &&
      other.parent == parent;
  
  int get hashCode => 37 * parent.hashCode + index;
}

/**
 * A type is simple, if it is either a
 * - [bool]
 * - [num]
 * - [String]
 * - A [List] of values of simple types
 * - A [Map] of Strings to values of simple types.
 */
void _checkSimpleType(dynamic value) {
  if (value == null)
    return;
  if (value is bool || value is num || value is String)
    return;
  if (value is List) {
    value.forEach(_checkSimpleType);
    return;
  }
  if (value is Map) {
    value.forEach((k, v) {
      if (k is! String)
        throw new JsonError.invalidKeyInMap(k);
      _checkSimpleType(v);
    });
    return;
  }
  throw new JsonError.notSimpleType(value);
  
}

/**
 * parses the path fragment with BNF
 *      path_index := '[' integer '] subpath'
 *      integer := '[0-9]+'
 */
Path parseIndex(_Tokenizer tokenizer) {
  assert(tokenizer.current != null && tokenizer.current.type == _Token.INDEX);
  var path = new IndexPath(int.parse(tokenizer.current.content));
  if (!tokenizer.moveNext() || tokenizer.current.type != _Token.R_BRACKET)
    throw new FormatException("Expected end of path index at ${tokenizer.index}");
  var subpath = _parseSubpath(tokenizer);
  if (subpath != null)
    return subpath ..root.parent = path;
  return path;
}

/**
 * Parses a path fragment with BNF
 *      field = '[a-zA-Z][a-zA-Z0-9]* subpath?'
 */
Path parseField(_Tokenizer tokenizer) {
  assert(tokenizer.current != null);
  if (tokenizer.current.type != _Token.FIELD_NAME)
    throw new FormatException("Expected a field name at ${tokenizer.index}");
  var path = new FieldPath(tokenizer.current.content);
  var subpath = _parseSubpath(tokenizer);
  if (subpath != null)
    return subpath ..root.parent = path;
  return path;
}
  
/**
 * Match the path fragment with BNF
 *      subpath := ( ( '.' field ) | path_index )?
 *      
 * Returns `null` if the subpath didn't match against the input.
 */
Path _parseSubpath(_Tokenizer tokenizer) {
  if (!tokenizer.moveNext())
    return null;
  switch(tokenizer.current.type) {
    case _Token.PERIOD:
      if (!tokenizer.moveNext())
        throw new FormatException("Incomplete path");
      return parseField(tokenizer);
    case _Token.L_BRACKET:
      if (!tokenizer.moveNext())
        throw new FormatException("Incomplete index clause");
      if (tokenizer.current.type != _Token.INDEX)
        throw new FormatException("Expected an integer at ${tokenizer.index}");
      var path = new IndexPath(int.parse(tokenizer.current.content));
      if (!tokenizer.moveNext() || tokenizer.current.type != _Token.R_BRACKET)
        throw new FormatException("Expected close of index at ${tokenizer.index}");
      var subpath = _parseSubpath(tokenizer)
          ..root._parent = path;
      return subpath;
    default:
      throw new FormatException("Invalid character at ${tokenizer.index}");
  }
}
 
class _Tokenizer extends Iterator<_Token> {
  static final FIELD_PATTERN = new RegExp(r'[a-zA-Z][a-zA-Z0-9_]*');
  static final INDEX_PATTERN = new RegExp(r'[0-9]+');
  static final WHITESPACE = new RegExp(r'\s+');
  
  final String input;
  int index;
  
  _Token _current = null;
  
  _Tokenizer(this.input):
    index = 0;
  
  _Token get current => _current;
  
  bool moveNext() {
    if (_current != null)
      index += _current.length;
    _skipWhitespace();
    if (index >= input.length)
      return false;
    
    _current = null;
    var match = FIELD_PATTERN.matchAsPrefix(input, index);
    if (match != null)
      _current = new _Token(_Token.FIELD_NAME, match.group(0));
    match = INDEX_PATTERN.matchAsPrefix(input, index);
    if (match != null)
      _current = new _Token(_Token.INDEX, match.group(0));
    if (input.startsWith('[', index))
      _current = new _Token(_Token.L_BRACKET, '[');
    if (input.startsWith(']', index))
      _current = new _Token(_Token.R_BRACKET, ']');
    if (input.startsWith('.', index))
      _current = new _Token(_Token.PERIOD, '.');
    
    return _current != null;
  }
  
  void _skipWhitespace() {
    var match = WHITESPACE.matchAsPrefix(input, index);
    if (match != null) {
      index += match.group(0).length;
    }
  }
}

class _Token {
  static const FIELD_NAME = 0;
  static const INDEX = 1;
  static const L_BRACKET = 2;
  static const R_BRACKET = 3;
  static const PERIOD = 4;
  
  final int type;
  final String content;
  
  int get length => content.length;
  
  _Token(this.type, this.content);
}