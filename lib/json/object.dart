library google_cloud_storage.json.object;

import 'dart:collection';

import 'error.dart';
import 'path.dart';
import 'selector.dart';

typedef T DelegateItem<T>(Path path);

class JsonObject {
  /**
   * The [JsonObject] which we delegates calls to the fields.
   * At most one of `_delegate` and `_json` will ever be set on
   * a `JsonObject`.
   *
   * A `delegate` is defined by a [Path] from the
   */
  final JsonObject _delegate;
  final Map<String,dynamic> _json;

  /**
   * Test whether `this` delegates to another object.
   */
  bool get _isDelegate => _delegate != null;

  /**
   * A reference to the [Map] which underlies all fields of `this`.
   * Fields in the [JsonObject] are the values of absolute paths
   * from the root of this map to a leaf.
   */
  Map<String,dynamic> get _rootJson =>
      (_delegate == null) ? _json : _delegate._rootJson;

  /**
   * Specifies a path from the roo
   */
  final Path _relPath;

  Path get _absPath {
    if (_delegate == null) return null;
    var delegateRoot = _delegate._absPath;
    return new Path.fromPath(_relPath, pathToRoot: delegateRoot);

  }

  /**
   * A selector of all the fields which are avaliable in the fetched object.
   */
  String get selector => _selector.toString();

  final Selector _selector;

  JsonObject.delegate(JsonObject delegate, var delegateField, {String selector}):
    this._(
        delegate,
        (delegateField is Path) ? delegateField : Path.parse(delegateField),
        null,
        Selector.parse(selector != null ? selector : '*')
    );

  JsonObject(Map<String,dynamic> json, {String selector: "*"}) :
    this._(
        null,
        null,
        json,
        Selector.parse(selector != null ? selector : '*')
    );

  JsonObject._(JsonObject delegate, Path this._relPath, Map<String,dynamic> json, Selector selector) :
    this._delegate = delegate,
    this._selector = selector,
    this._json = selector.select(json != null ? json : new Map()) {
    if (_delegate != null) {
      //Make sure we are in the selection
      if (!_delegate._hasFieldPath(_relPath)) {
        throw new NotInSelectionError(_delegate, _relPath);
      }
      //Make sure there is an object at the path
      var absPath = _absPath;
      var value = _absPath.getValue(_rootJson);
      if (value == null) {
        _absPath.setValue(_rootJson, {});
      }
    }
  }

  /**
   * The object as a `JSON` map. Relative paths are defined relative to the
   * output of the `toJson` function.
   */
  Map<String,dynamic> toJson() =>
      _delegate == null ?
          _selector.select(_rootJson) :
          _absPath.getValue(_rootJson);


  /**
   * Return a path from the root of `_json` to the specified field.
   */
  Path _absolutePath(String field) {
    return new FieldPath(field, parent: _absPath);
  }

  /**
   * Gets the path relative to the root of the delegate's `json`.
   */
  Path _relativePath(String field) {
    return new FieldPath(field, parent: _absPath);
  }

  /**
   * Check whether the field satisfies both the selector for the current field
   * and the selector for the delegate (if the object has one).
   */
  bool _hasFieldPath(Path pathToField) {
    var hasField = _selector.isPathInSelection(pathToField);
    if (_isDelegate) {
      hasField = hasField && _delegate._hasFieldPath(new Path.fromPath(pathToField, pathToRoot: _relPath));
    }
    return hasField;
  }

  bool hasField(String name) => _hasFieldPath(new FieldPath(name));

  dynamic getField(String name, {dynamic defaultValue()}) {
    if (!_hasFieldPath(new FieldPath(name)))
      throw new NotInSelectionError(this, name);
    var value = _absolutePath(name).getValue(_rootJson);
    if (value == null && defaultValue != null) {
      value = defaultValue();
      setField(name, value);
    }
    return value;
  }

  void setField(String name, dynamic value) {
    if (!_hasFieldPath(new FieldPath(name)))
      throw new NotInSelectionError(this, name);
    if (value is JsonObject)
      value = value.toJson();
    _absolutePath(name).setValue(_rootJson, value);
  }

  toString() => toJson().toString();

}

class JsonList<T extends JsonObject> extends ListBase<T> {
  final JsonObject _delegate;

  /**
   * The field in the delegate object which contains this list.
   */
  final String _field;

  final DelegateItem<T> _delegateItem;

  JsonList(JsonObject this._delegate, String this._field, this._delegateItem) {
    //Make sure there is a `JsonList` at path.
    if (_delegate.getField(_field) == null)
      _delegate.setField(_field, []);
  }

  //A reference to the raw list of json values
  List<Map<String,dynamic>> get _json => _delegate.getField(_field);

  void add(T value) => _json.add(value.toJson());
  void addAll(Iterable<T> values) => _json.addAll(values.map((v) => v.toJson()));

  T operator [](int i) {
    if (i < 0 || i >= length) {
      throw new RangeError.range(i, 0, length - 1);
    }
    var path = new IndexPath(i, parent: new FieldPath(_field));
    return _delegateItem(path);
  }

  void operator []=(int i, T value) {
    if (i < 0 || i >= length) {
      throw new RangeError.range(i, 0, length - 1);
    }
    _json[i] = value.toJson();
  }

  int get length => _json.length;
  set length(int value) => _json.length = value;

}