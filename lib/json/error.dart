library google_cloud_storage.json.error;

class JsonError extends Error {
  final String message;
  JsonError(String this.message);
  
  JsonError.expectedPathSelector(String selector) :
    this("Must be a path selector '$selector'");
  
  JsonError.invalidKeyInMap(dynamic key) :
    this("Keys in json maps must be Strings");
  
  JsonError.notSimpleType(dynamic value) :
    this("Invalid type for value in json object ($value)");
  
  JsonError.expectedMap(dynamic parentValue):
    this("$parentValue should be a map");
  
  JsonError.expectedListValue(String path):
    this("Expected a list at '$path'");
  
  String toString() => "JsonError: $message";
}

class PathError extends Error {
  final String message;
  PathError(String this.message);
  
  PathError.expectedMap(dynamic value):
    this ('expected a map ($value)');
  
  PathError.expectedList(dynamic value):
    this ('expected a list ($value)');
  
  String toString() => "Path error: $message";
}

/**
 * An error which is thrown when attempting to access a field which
 * is not in the selection which was used to instantiate a `JSON` object.
 * 
 * Used mainly so that fields aren't accidentally nulled when submitting
 * a patch request of a `JsonObject` to a json web service.
 */
class NotInSelectionError extends Error {
  final receiver;
  final path;
  
  NotInSelectionError(this.receiver, this.path);
  
  String toString() => "$receiver path $path was unselected";
}