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

  bool get isInit => type == TOKEN_INIT;
  bool get isComplete => type == TOKEN_COMPLETE;

  /**
   * The endpoint of the upload service
   */
  final Uri uploadUri;

  /**
   * The range of bytes that have been sucessfully uploaded.
   * *Note*: The specified range is a closed range, inclusive of the first and last byte positions
   * in accordance with `W3C` specifications.
   */
  final Range range;

  /**
   * A rpc which can be used to get the metadata of the object rather than failing
   */
  final RpcRequest getObjectRequest;

  /**
   * The selector of the completed upload metadata
   */
  String get resultSelector => getObjectRequest.query['fields'];

  ResumeToken(this.type, this.uploadUri, this.range, this.getObjectRequest);

  ResumeToken.fromToken(ResumeToken token, this.type, this.range):
    this.uploadUri = token.uploadUri,
    this.getObjectRequest = token.getObjectRequest;

  factory ResumeToken.fromJson(Map<String,dynamic> json) {

    var type = json['type'];

    var range;
    //FIXME: Shouldn't raise a FormatException in the first place
    print('type: ${json['type']}');
    if (type == TOKEN_INIT) {
      range = new Range(0, -1);
    } else {
      range = Range.parse(json['range']);
    }
    return new ResumeToken(
        type,
        Uri.parse(json['url']),
        range,
        new RpcRequest.fromJson(json['rpc'])
    );
  }

  toJson() =>
      { 'type': type,
         'url': uploadUri.toString(),
         'range': range.toString(),
         'rpc': getObjectRequest.toJson()
      };
}