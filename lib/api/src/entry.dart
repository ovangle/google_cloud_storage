part of google_cloud_storage.api;

abstract class StorageEntry extends JsonObject {
  /**
   * Access controls on the entry.
   */
  List<AccessControls> get acl;

  /**
   * The name of the entry.
   */
  String get name => getField("name");

  /**
   * The `HTTP 1.1` [entity tag][0] of the bucket
   * [0]: http://tools.ietf.org/html/rfc2616#section-3.11
   */
  String get etag => getField("etag");

  /**
   * The id of the entry.
   */
  String get id => getField("id");

  /**
   * The kind of the entry.
   */
  String get kind => getField("kind");

  /**
   * The metageneration of the entry
   */
  int get metageneration => getField("metageneration");

  /**
   * The URI of this entry.
   */
  String get selfLink => getField("selfLink");

  StorageEntry._(Map<String,dynamic> json, {String selector: "*"}) :
    super(json, selector: selector);
}

/**
 * A [StorageBucket] is represents a storage container in google cloud storage.
 * There is a single namespace shared by all buckets.
 *
 * Buckets contain [StorageObject]s which can be accessed by their own methods.
 */
class StorageBucket extends StorageEntry {
  @override
  List<BucketAccessControls> get acl =>
      new JsonList<BucketAccessControls>(
          this, "acl",
          (path) => new BucketAccessControls._delegate(this, "acl"));

  /**
   * The resource's [Cross-Origin Resource Sharing] configuration
   */
  List<CorsConfiguration> get cors =>
      new JsonList(
          this, "posts",
          (path) => new CorsConfiguration._delegate(this, "path"));

  /**
   * The default access controls for objects in this [StorageBucket]
   * which have no acl is provided.
   */
  List<ObjectAccessControls> get defaultObjectAcl =>
      new JsonList(
          this, "posts",
          (path) => new ObjectAccessControls._delegate(this, "defaultObjectAcl"));

  /**
   * The lifecycle configuration of [StorageObject]s in the bucket.
   */
  Lifecycle get lifecycle => new Lifecycle._delegate(this, "lifecycle");
  set lifecycle(Lifecycle value) => setField("lifecycle", value);

  /**
   * The geographic storage location of the bucket.
   */
  Location get location => new Location.fromString(getField("location"));
  set location(Location location) => setField("location", location.toString());

  /**
   * The [LoggingConfiguration] of the bucket.
   */
  LoggingConfiguration get logging => new LoggingConfiguration._delegate(this, "logging");

  /**
   * The time the bucket was created.
   */
  DateTime get timeCreated {
    var value = getField("timeCreated");
    if (value != null)
      return DateTime.parse(value);
    return null;
  }

  /**
   * The owner of the bucket. This will always be the project team's owner group.
   */
  String get owner => getField("owner");

  StorageClass get storageClass => new StorageClass.fromString(getField("storageClass"));
  set storageClass(StorageClass storageClass) =>
      setField("storageClass", storageClass != null ? storageClass.toString() : null);

  /**
   * The bucket's versioning configuration.
   */
  VersioningConfiguration get versioning => new VersioningConfiguration._delegate(this, "versioning");
  set versioning(VersioningConfiguration versioning) => setField("versioning", versioning);

  WebsiteConfiguration get website => new WebsiteConfiguration._delegate(this, "website");
  set website(WebsiteConfiguration website) => setField("website", website);

  static const _CREATE_SELECTOR =
      "name,acl,cors,defaultObjectAcl,lifecycle,location,logging,storageClass,versioning,website";

  StorageBucket(String name, {String selector: _CREATE_SELECTOR}) :
    this._({"name" : name}, selector: selector);

  StorageBucket._(Map<String,dynamic> json, {String selector}): super._(json, selector: selector);
}

abstract class StorageObject extends StorageEntry {
  /**
   * The name of the bucket containing this [StorageObject]
   */
  String get bucket => getField("bucket");


  /**
   * Number of underlying components that make up this object.
   */
  int get componentCount => getField("componentCount");

  /**
   * [Cache-Control][0] directives for the object data.
   *
   * [0]: https://tools.ietf.org/html/rfc2616#section-14.9
   */
  String get cacheControls => getField("cacheControls");
  set cacheControls(String cacheControls) => setField("cacheControls", cacheControls);

  /**
   * [Content-Disposition][0] of the object data.
   *
   * [0]: https://tools.ietf.org/html/rfc6266
   */
  String get contentDisposition => getField("contentDisposition");
  set contentDisposition(String contentDisposition) => setField("contentDisposition", contentDisposition);

  /**
   * [Content-Encoding][0] of the object data.
   *
   * [0]: https://tools.ietf.org/html/rfc2616#section-14.11
   */
  String get contentEncoding => getField("contentEncoding");
  set contentEncoding(String contentEncoding) => setField("contentEncoding", contentEncoding);

  /**
   * [Content-Language][0] of the object data.
   *
   * [0]: http://www.loc.gov/standards/iso639-2/php/code_list.php
   */
  String get contentLanguage => getField("contentLanguage");
  set contentLanguage(String contentLanguage) => setField("contentLanguage", contentLanguage);

  /**
   * [Content-Type][0] of the object data.
   *
   * [0]: https://tools.ietf.org/html/rfc2616#section-14.17
   */
  String get contentType => getField("contentType");
  set contentType(String contentType) => setField("contentType", contentType);

  /**
   * [CRC32c] checksum, encoded using base64
   */
  String get crc32c => getField("crc32c");
  set crc32c(String checksum) => setField("crc32c", checksum);

  /**
   * The content generation of the object. Used for object versioning.
   */
  int get generation => getField("generation");

  /**
   * MD5 hash of the object, encoded using base64
   */
  String get md5Hash => getField("md5Hash");
  set md5Hash(String hash) => setField("md5Hash", hash);

  /**
   * Media download link
   */
  String get mediaLink => getField("mediaLink");

  /**
   * User provided metadata for the object.
   */
  Map<String,String> get metadata => getField("metadata");

  /**
   * The owner of the object. This will always be the uploader of the object.
   */
  String get owner => getField("owner");

  /**
   * The [Content-Length][0] of the data in bytes.
   *
   * [0]: https://tools.ietf.org/html/rfc2616#section-14.13
   */
  int get size => getField("size");

  /**
   * Deletion time of the object. Will only be returned if this version of
   * the object has been deleted.
   */
  DateTime get timeDeleted {
    var value = getField("timeDeleted");
    if (value != null)
      return DateTime.parse(value);
    return null;
  }

  /**
   * Last time the object was modified.
   */
  DateTime get updated {
    var value = getField("updated");
    if (value != null)
      return DateTime.parse(value);
    return null;
  }

  StorageObject._(Map<String,dynamic> json, {String selector: "*"}): super._(json, selector: selector);
}
