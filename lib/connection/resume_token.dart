library resume_token;

import '../utils/content_range.dart';
import 'rpc.dart';

/**
 * A token which can be used to resume an upload from the point in the source where it failed.
 */
class ResumeToken {
  /**
   * The token obtained from the `uploadObject` method
   */
  static const TOKEN_INIT = 0;
  /**
   * The token obtained when a upload request was interrupted
   */
  static const TOKEN_INTERRUPTED = 1;
  /**
   * A token obtained when the upload is complete.
   */
  static const TOKEN_COMPLETE = 2;

  /**
   * The type of the token. One of [TOKEN_INIT], [TOKEN_INTERRUPTED] or [TOKEN_COMPLETE]
   */
  final int type;

  /**
   * The range (inclusive of start and end bytes) that has already been uploaded
   * to the server when this token was created.
   *
   * A `null` range indicates that no bytes have yet been uploaded to the server.
   */
  final Range range;

  final RpcResponse rpcResponse;

  final String selector;

  bool get isInit => type == TOKEN_INIT;
  bool get isComplete => type == TOKEN_COMPLETE;

  /**
   * The endpoint of the upload service
   */
  final Uri uploadUri;

  ResumeToken(this.type, this.uploadUri, this.selector, {this.range}) : rpcResponse = null;

  ResumeToken.fromToken(ResumeToken token, this.type, {this.range, this.rpcResponse}):
    this.uploadUri = token.uploadUri,
    this.selector = token.selector;

  /**
   * Deserialize a [ResumeToken].
   */
  factory ResumeToken.fromJson(Map<String,dynamic> json) {
    var type = json['type'];
    if (type == null) {
      throw new TokenSerializationException("No 'type'");
    }
    if (type < 0 || type > 2) {
      throw new TokenSerializationException("Invalid value for type field");
    }
    if (json['uploadUri'] == null)
      throw new TokenSerializationException("No 'uploadUri'");
    return new ResumeToken(
        type,
        Uri.parse(json['uploadUri']),
        json['selector'] != null ? json['selector'] : '*',
        range: (json['range'] != null) ? Range.parse(json['range']) : null
    );
  }

  /**
   * Serialize a [ResumeToken] as a JSON map.
   */
  Map<String,dynamic> toJson() {
    var json = {
        'type': type,
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