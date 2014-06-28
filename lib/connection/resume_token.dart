library resume_token;

import 'dart:async';

import '../utils/content_range.dart';
import '../api/api.dart';
import 'rpc.dart';

/**
 * A token which can be used to resume an upload from the point in the source where it failed.
 *
 * The token is obtained as a result of calling [uploadObject] method on [Connection]. It
 * contains information necessary to resume a failed or interrupted upload. The token can be
 * serialized and deserialized. This is most useful for situations where upload information
 * needs to be preserved across browser sessions or persisted into some storage.
 */
class ResumeToken {
  /**
   * The range (inclusive of start and end bytes) that has already been uploaded
   * to the server when this token was created.
   *
   * A `null` range indicates that no bytes have yet been uploaded to the server.
   */
  final Range range;

  /**
   * Selector specifying a subset of fields to include in the response. For more information see
   * https://developers.google.com/storage/docs/json_api/v1/how-tos/performance#partial.
   */
  final String selector;

  /**
   * Completer which is fired when the upload has successfully finished.
   * Completes with an error if the upload was somehow interrupted.
   */
  final Completer<StorageObject> _completer;

  bool get isCompleted {
    if (_completer == null) throw new TokenSerializationException(
        "Deserialized token cannot be completed. "
        "Use 'connection.resumeUpload' to obtain a new, completable token."
    );
    return _completer.isCompleted;
  }

  Future<StorageObject> get done {
    if (_completer == null) throw new TokenSerializationException(
        "Deserialized token cannot be completed. "
        "Use 'connection.resumeUpload' to obtain a new, completable token."
    );
    return _completer.future;
  }

  /**
   * The endpoint of the upload service
   */
  final Uri uploadUri;

  ResumeToken(this.uploadUri, {completer, this.range, this.selector: '*'})
      : _completer = completer;


  ResumeToken.fromToken(ResumeToken token, {completer, this.range}):
    _completer = completer,
    this.uploadUri = token.uploadUri,
    this.selector = token.selector;

  /**
   * Deserialize a [ResumeToken].
   */
  factory ResumeToken.fromJson(Map<String,dynamic> json) {
    if (json['uploadUri'] == null)
      throw new TokenSerializationException("No 'uploadUri'");
    if (json['done'] != null)
      throw new TokenSerializationException("Invalid resume token. 'done' attribute found.");
    return new ResumeToken(
        Uri.parse(json['uploadUri']),
        selector: json['selector'],
        range: (json['range'] != null) ? Range.parse(json['range']) : null);
  }

  /**
   * Serialize a [ResumeToken] as a JSON map.
   */
  Map<String,dynamic> toJson() {
    var json = {
        'uploadUri': uploadUri.toString(),
        'selector': selector
    };
    if (range != null)
      json['range'] = range.toString();
    return json;
  }
}

class TokenSerializationException implements Exception {
  final String message;

  TokenSerializationException(this.message);

  toString() => message;

}