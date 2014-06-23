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

  final Range range;

  final rpcResponse;

  bool get isInit => type == TOKEN_INIT;
  bool get isComplete => type == TOKEN_COMPLETE;

  /**
   * The endpoint of the upload service
   */
  final Uri uploadUri;

  ResumeToken(this.type, this.uploadUri, {this.range}) : rpcResponse = null;

  ResumeToken.fromToken(ResumeToken token, this.type, {this.range, this.rpcResponse}):
    this.uploadUri = token.uploadUri;
}