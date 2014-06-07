/**
 * Miscellaneous api classes.
 */
part of google_cloud_storage.api;

/**
 * The geographic storage location for a [StorageBucket].
 */
class Location {
  /**
   * Asia
   */
  static const ASIA = const Location._("ASIA");
  /**
   * The United States
   */
  static const US = const Location._("US");
  /**
   * The European union
   */
  static const EU = const Location._("EU");

  /**
   * Eastern asia-pacific.
   *
   * *NOTE*
   * This location is a [Regional Bucket][0].
   * Regional Buckets are experimental and data stored in these buckets is not
   * subject to the usual SLA. See the documentation for additional information.
   *
   * [0]: https://developers.google.com/storage/docs/regional-buckets
   */
  static const ASIA_EAST1 = const Location._("US-EAST1");

  /**
   * Eastern united states
   *
   * *NOTE*
   * This location is a [Regional Bucket][0].
   * Regional Buckets are experimental and data stored in these buckets is not
   * subject to the usual SLA. See the documentation for additional information.
   *
   * [0]: https://developers.google.com/storage/docs/regional-buckets
   */
  static const US_EAST1 = const Location._("US-EAST1");

  /**
   * Eastern united states
   *
   * *NOTE*
   * This location is a [Regional Bucket][0].
   * Regional Buckets are experimental and data stored in these buckets is not
   * subject to the usual SLA. See the documentation for additional information.
   *
   * [0]: https://developers.google.com/storage/docs/regional-buckets
   */
  static const US_EAST2 = const Location._("US-EAST2");

  /**
   * Eastern united states
   *
   * *NOTE*
   * This location is a [Regional Bucket][0].
   * Regional Buckets are experimental and data stored in these buckets is not
   * subject to the usual SLA. See the documentation for additional information.
   *
   * [0]: https://developers.google.com/storage/docs/regional-buckets
   */
  static const US_EAST3 = const Location._("US-EAST3");

  /**
   * Central united states
   *
   * *NOTE*
   * This location is a [Regional Bucket][0].
   * Regional Buckets are experimental and data stored in these buckets is not
   * subject to the usual SLA. See the documentation for additional information.
   *
   * [0]: https://developers.google.com/storage/docs/regional-buckets
   */
  static const US_CENTRAL1 = const Location._("US-CENTRAL1");

  /**
   * Central united states
   *
   * *NOTE*
   * This location is a [Regional Bucket][0].
   * Regional Buckets are experimental and data stored in these buckets is not
   * subject to the usual SLA. See the documentation for additional information.
   *
   * [0]: https://developers.google.com/storage/docs/regional-buckets
   */
  static const US_CENTRAL2 = const Location._("US-CENTRAL2");

  /**
   * West united states
   *
   * *NOTE*
   * This location is a [Regional Bucket][0].
   * Regional Buckets are experimental and data stored in these buckets is not
   * subject to the usual SLA. See the documentation for additional information.
   *
   * [0]: https://developers.google.com/storage/docs/regional-buckets
   */
  static const US_WEST1 = const Location._("US-WEST1");

  static const values = const [US, EU, US_EAST1, US_EAST2, US_EAST3, US_CENTRAL1, US_CENTRAL2, US_WEST1];

  final String _value;
  const Location._(String this._value);

  factory Location.fromString(String str) {
    if (str == null) return null;
    try {
      values.singleWhere((v) => v._value == str);
    } on StateError {
      //It is entirely possible that this enum is not up to date
      //with the current available storage locations.
      return new Location._(str);
    }
  }

  String toString() => _value;
}

/**
 * A [Bucket]s lifecycle configuration.
 */
class Lifecycle extends JsonObject {
  /**
   * The list of lifecycle management rules which apply to the [Bucket]
   */
  List<LifecycleRule> get rule =>
      new JsonList(
          this, "rule",
          (path) => new LifecycleRule._delegate(this, path));

  Lifecycle._delegate(JsonObject obj, var path, {String selector: "*"}):
    super.delegate(obj, path, selector: selector);
}

class LifecycleRule extends JsonObject {
  /**
   * The action to take when the condition is satisfied.
   */
  LifecycleAction get action => new LifecycleAction.fromJson(getField("action"));

  LifecycleCondition get condition => new LifecycleCondition.fromJson(getField("condition"));

  LifecycleRule(LifecycleAction action, LifecycleCondition condition):
    super({'action':action.toJson(), 'condition':condition.toJson()});

  LifecycleRule._(Map<String,dynamic> json, {String selector: "*"}):
    super(json, selector: "*");

  LifecycleRule._delegate(JsonObject obj, var field, {String selector: "*"}):
    super.delegate(obj, field, selector: selector);
}

/**
 * An action to take when the condition of a rule is satisfied.
 */
class LifecycleAction {
  static const DELETE = const LifecycleAction._("Delete");

  static const List values = const [ DELETE ];

  final String _type;
  const LifecycleAction._(String this._type);

  factory LifecycleAction.fromJson(Map<String,dynamic> json) {
    try {
      return values.singleWhere((v) => v._type == json['type']);
    } on StateError {
      throw 'Invalid value for lifecycle action: ${json['type']}';
    }
  }

  toJson() => { 'type' : _type };

  toString() => _type;
}

/**
 * A satisfiable condition which will trigger a [LifecycleAction].
 * Exactly one of [:age:], [:createdBefore:], [:isLive:], [:numFewerVersions:]
 * should be non-null.
 */
class LifecycleCondition {
  /**
   * This condition is satisfied when an object reaches the given age (in days).
   */
  final int age;
  /**
   * A date, specified as `YYYY-MM-DD`.
   * This condition is satisfied when an object reaches the specified age.
   */
  final String createdBefore;
  /**
   * Relevant only for versioned objects.
   * If the value is `true`, the condition matches live objects.
   * If the value is `false`, the condition matches archived objects.
   */
  final bool isLive;
  /**
   * Relevant only for versioned objects.
   * This condition is satisfied if there are at least [:numFewerVersions:]
   * newer than this version of the object.
   */
  final int numFewerVersions;

  LifecycleCondition({this.age, this.createdBefore, this.isLive, this.numFewerVersions});

  LifecycleCondition.fromJson(Map<String,dynamic> json) :
    this( age: json['age'],
          createdBefore: json['createdBefore'],
          isLive: json['isLive'],
          numFewerVersions: json['numFewerVersions']
    );

  Map<String,dynamic> toJson() {
    var json = new Map<String,dynamic>();
    if (age != null)
      json['age'] = age;
    if (createdBefore != null)
      json['createdBefore'] = age;
    if (isLive != null)
      json['isLive'] = isLive;
    if (numFewerVersions != null)
      json['numFewerVersions'] = numFewerVersions;
    return json;
  }
}

/**
 * Configuration of File entries which determine access via [Cross-Origin Resource Sharing][0]
 * requests
 * [0]:http://www.w3.org/TR/cors/
 */
class CorsConfiguration extends JsonObject {
  /**
   * The value, in seconds, to return the [Access Control-Max-Age-header][0]
   * used in preflight responses.
   *
   * [0]:http://www.w3.org/TR/cors/#access-control-max-age-response-header
   */
  int get maxAgeSeconds => getField("maxAgeSeconds");
  set maxAgeSeconds(int value) => setField("maxAgeSeconds", value);
  /**
   * The list of `HTTP` methods on which to include `CORS` response headers
   * `"*"` is a permitted in the list of methods and is interpreted as `"any method"`
   */
  List<String> get method => getField("method");
  /**
   * The list of ofigins eligable to receive `CORS` response headers.
   * `"*"` is permitted in the list of origins and is interpreted as `"any origin"`
   */
  List<String> get origin => getField("origin");
  /**
   * The list of `HTTP` headers other than the [simple response headers][0] which give
   * permission to the user agent to share across domains
   * [0]: http://www.w3.org/TR/cors/#simple-response-header
   */
  List<String> get responseHeader => getField("responseHeader");

  CorsConfiguration() :
    super({'method':[], 'origin':[], 'responseHeader':[]});

  CorsConfiguration._(Map<String,dynamic> json, {String selector: "*"}):
    super(json, selector: selector);

  CorsConfiguration._delegate(JsonObject obj, /* String | Path */ field, {String selector: "*"}):
    super.delegate(obj, field, selector: selector);
}


/**
 * A [LoggingConfiguration] specifies the bucket where logs should be placed
 * and a prefix to apply to log objects.
 */
class LoggingConfiguration extends JsonObject {
  /**
   * The name of the [StorageBucket] in which the logs should be placed.
   */
  String get logBucket => getField("logBucket");
  set logBucket(String logBucket) => setField("logBucket", logBucket);
  /**
   * A prefix to apply to log objects.
   */
  String get logObjectPrefix => getField("logObjectPrefix");
  set logObjectPrefix(String prefix) => setField("logObjectPrefix", prefix);

  LoggingConfiguration(): super({});
  LoggingConfiguration._(Map<String,dynamic> json, {String selector: "*"}): super(json, selector: selector);
  LoggingConfiguration._delegate(JsonObject obj, var path, {String selector: "*"}): super.delegate(obj, path, selector: selector);
}

class StorageClass {
  static const STANDARD = const StorageClass._("STANDARD");
  static const DURABLE_REDUCED_AVAILABILITY = const StorageClass._("DURABLE_REDUCED_AVAILABILITY");

  static const List values = const [ STANDARD, DURABLE_REDUCED_AVAILABILITY ];

  final String _value;
  const StorageClass._(String this._value);

  factory StorageClass.fromString(String storageClass) {
    if (storageClass == null) return null;
    try {
      return values.singleWhere((v) => v._value == storageClass);
    } on StateError {
      throw 'Invalid storage class: $storageClass';
    }
  }

  String toString() => _value;
}

/**
 * Controls whether a [StorageEntry] is versioned.
 */
class VersioningConfiguration extends JsonObject {
  /**
   * While set to `true`, versioning is enabled.
   */
  bool get enabled => getField("enabled");
  set enabled(bool value) => setField("enabled", value);

  VersioningConfiguration({bool enabled: false}): super({'enabled':enabled});

  VersioningConfiguration._delegate(JsonObject obj, var path, {String selector: "*"}):
    super.delegate(obj, path, selector: selector);
}

/**
 * Controls where to look for missing objects in a [StorageBucket].
 */
class WebsiteConfiguration extends JsonObject {
  /**
   * The directory index where missing objects are treated as potential entities.
   */
  String get mainPageSuffix => getField("mainPageSuffix");
  set mainPageSuffix(String value) => setField("mainPageSuffix", value);
  /**
   * The custom object to return when a requested is not found.
   */
  String get notFoundPage => getField("notFoundPage");
  set notFoundPage(String value) => setField("notFoundPage", value);

  WebsiteConfiguration({String mainPageSuffix, String notFoundPage}):
    this._({'mainPageSuffix': mainPageSuffix, 'notFoundPage': notFoundPage});

  WebsiteConfiguration._(Map<String,dynamic> json, {String selector: "*"}):
    super(json, selector: selector);

  WebsiteConfiguration._delegate(JsonObject obj, var path, {String selector: "*"}):
    super.delegate(obj, path, selector: selector);
}



