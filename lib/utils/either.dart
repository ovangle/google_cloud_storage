library either;

import 'package:quiver/core.dart' show hash2;

//An identity function
_id(var value) => value;

/**
  * For each of the items in [:eithers:], yields the inner value
  * of the item if it is a right value, otherwise skips the value.
  */
Iterable whereRight(Iterable<Either> eithers) =>
     eithers.expand((either) => either.isRight ? [either.right] : const []);

/**
 * For each of the items in [:eithers:], yields the inner value
 * of the item if it is a left value, otherwise skips the value.
 */
Iterable whereLeft(Iterable<Either> eithers) =>
     eithers.expand((either) => either.isLeft ? [either.left] : const []);


/**
 * A simple sum of two distinct types.
 */
class Either<L,R> {
  final L _left;
  final R _right;

  /**
   * If `this` is a left value, returns the inner value of the
   * [Either]. Otherwise, throws a [StateError].
   */
  L get left {
    if (_left == null)
      throw new StateError("either is a right value");
    return _left;
  }
  /**
   * If `this` is a right value, returns the inner value of the
   * [Either]. Otherwise, throws a [StateError].
   */
  R get right {
    if (_right == null)
      throw new StateError("either is a left value");
    return _right;
  }

  /**
   * Test whether `this` is a left value.
   */
  bool get isLeft => _right == null;
  /**
   * Test whether `this` is a right value.
   */
  bool get isRight => _left == null;

  Either._(this._left, this._right);

  /**
   * Creates a new left value.
   */
  factory Either.ofLeft(L value) {
    if (value == null)
      throw new ArgumentError("value is null");
    return new Either._(value, null);
  }
  /**
   * Create a new right value.
   */
  factory Either.ofRight(R value) {
    if (value == null)
      throw new ArgumentError("value is null");
    return new Either._(null, value);
  }

  /**
   * Test whether the value should be a left or right value and create an appropriate
   * [Either].
   *
   * [:test:] should return `true` on right values and `false` on left values
   */
  factory Either.branch(dynamic value, bool test(value)) =>
      test(value) ? new Either.ofRight(value) : new Either.ofLeft(value);

  /**
   * If `this` is the right value, returns the result of applying [:ifRight:]
   * to the value. Otherwise applies the [:ifLeft:].
   *
   * A parameter which is not provided is treated as the identity function.
   */
  dynamic fold({dynamic ifLeft(L value): _id, dynamic ifRight(R value): _id}) =>
      isRight ? ifRight(_right) : ifLeft(_left);

  /**
   * If `this` is a right value, applies the function to the underlying value
   * and returns a new right value containing the result of the function.
   * When applied to a left value, just returns `this`.
   */
  Either<L,dynamic> map(dynamic f(R value)) =>
      isRight ? new Either._(_left, f(_right)) : this;

  /**
   * Returns a new [Either] with the left and right values swapped.
   */
  Either<R,L> swap() => new Either._(_right, _left);

  bool operator ==(Object other) =>
      other is Either &&
      other._left == _left &&
      other._right == _right;

  int get hashCode => hash2(_left, _right);

  String toString() => isLeft ? "Left[$_left]": "Right[$_right]";

  Map<String,dynamic> toJson() => {'left': _left, 'right': _right };
}
