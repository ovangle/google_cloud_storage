part of google_cloud_storage.api;


/**
 * Control who has access to a [StorageEntry]
 */
abstract class AccessControls extends JsonObject {

  /**
   * The name of the bucket these controls apply to.
   */
  String get bucket => getField("bucket");

  /**
   * The ID of the access controls.
   */
  String get id => getField("id");

  /**
   * The kind of item this is.
   */
  String get kind => getField("kind");
  /**
   * The `HTTP 1.1` [Entity tag][0] for the entity.
   * [0]: http://tools.ietf.org/html/rfc2616#section-3.11
   */
   String get etag => getField("etag");

  /**
   * The link to the access control entry.
   */
  String get selfLink => getField("selfLink");

  /**
   * The domain associated with the entity, if any
   */
  String get domain => getField("domain");

  /**
   * The email address associated with the entity, if any.
   */
  String get email => getField("email");

  /**
   * The `ID` for the entity, if any.
   */
  String get entityId => getField("entityId");

  /**
   * The entity holding the permission, in one of the following forms:
   * - `user-userId`
   * - `user-email`
   * - `group-groupId`
   * - `group-email`
   * - `domain-domain`
   * - `allUsers`
   * - `allAuthenticatedUsers`
   *
   *
   * Examples:
   * The user `liz@example.com` would be `user-liz@example.com`.
   * The group `example@googlegroups.com` would be `group-example@googlegroups.com`.
   * To refer to all members of the Google Apps for Business domain `example.com`, the entity would be `domain-example.com`.
   */
  String get entity => getField("entity");
  set entity(String entity) => setField("entity", entity);

  /**
   * The access permission for the entity.
   */
  PermissionRole get role => new PermissionRole.fromString(getField("role"));
  set role(PermissionRole role) => setField("role", role.toString());

  /**
   * Create a new [AccessControls] for the specified [Bucket].
   */
  AccessControls(String bucket, String entity, PermissionRole role, String selector):
    super({'entity': entity, 'role': role.toString()}, selector: selector);

  AccessControls._(Map<String,dynamic> json, {String selector: "*"}):
    super(json, selector: selector);

  AccessControls._delegate(JsonObject obj, var path, {String selector: "*"}):
    super.delegate(obj, path, selector: selector);
}

/**
 * [BucketAccessControls] specify who has access to the data and to what extent.
 */
class BucketAccessControls extends AccessControls {
  BucketAccessControls.fromJson(Map<String,dynamic> json, {String selector: "*"}):
    super._(json, selector: selector);

  BucketAccessControls._delegate(JsonObject obj, var path, {String selector: "*"}):
    super._delegate(obj, path, selector: selector);

  BucketAccessControls(): this.fromJson({});
}

class ObjectAccessControls extends AccessControls {

  /**
   * The name of the object to which these controls apply.
   */
  String get object => getField("object");

  /**
   * The content generation of the object.
   */
  int get generation => getField("generation");

  ObjectAccessControls.fromJson(Map<String,dynamic> json, {String selector: "*"}):
    super._(json, selector:selector);

  ObjectAccessControls._delegate(JsonObject obj, var path, {String selector: "*"}):
    super._delegate(obj, path, selector: selector);

  /**
   * A new [ObjectAccessControls] which is applied to every object created
   * in the specified [:bucket:]
   */
  ObjectAccessControls.defaultForBucket(String bucket): this.fromJson({});

  /**
   * A new [ObjectAccessControls] for the given object
   */
  ObjectAccessControls(String bucket, String object): this.fromJson({'bucket': bucket, 'object': object});
}

/**
 * Represents the level of access to a [StorageEntry].
 */
class PermissionRole {
  /**
   * Readers can get a [StorageEntry], though the [:acl:] property will not be revealed.
   */
  static const READER = const PermissionRole._("READER");
  /**
   * Writers are readers, and can insert objects into a bucket.
   * **NOTE**: This role is invalid on object permissions.
   */
  static const WRITER = const PermissionRole._("WRITER");
  /**
   * Owners are readers and writers, and can get and update the [:acl:] property
   * of an entry.
   */
  static const OWNER = const PermissionRole._("OWNER");


  static const List values = const [READER, WRITER, OWNER];
  final String _value;
  const PermissionRole._(String this._value);

  factory PermissionRole.fromString(String value) {
    if (value == null) return null;
    try {
      return values.singleWhere((v) => v._value == value);
    } on StateError {
      throw 'Invalid role: $value';
    }
  }

  String toString() => _value;
}
