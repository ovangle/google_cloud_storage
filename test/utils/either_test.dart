library either_test;

import 'package:unittest/unittest.dart';
import '../../lib/utils/either.dart';

void main() {
  group("either:", () {
    test("should throw when trying to create an either with a `null` value", () {
      expect(() => new Either.ofLeft(null), throws);
      expect(() => new Either.ofRight(null), throws);
    });
    group("a right value", () {
      var rightVal = new Either.ofRight(4);
      test("should apply the function when mapped over", () {
        expect(rightVal.map((v) => v + 1), new Either.ofRight(5));
      });
      test("should be a left value when swapped", () {
        expect(rightVal.swap(), new Either.ofLeft(4));
      });
      test("should apply the right function when folded", () {
        expect(rightVal.fold(ifLeft: (v) => v - 1, ifRight: (v) => v + 1), 5);
      });
    });
    group("a left value", () {
      var leftVal = new Either.ofLeft(4);
      test("should not apply the function when mapped over", () {
        expect(leftVal.map((v) => v + 1), new Either.ofLeft(4));
      });
      test("should be a right value when swapped", () {
        expect(leftVal.swap(), new Either.ofRight(4));
      });
      test("should apply the left function when folded", () {
        expect(leftVal.fold(ifLeft: (v) => v - 1, ifRight: (v) => v + 1), 3);
      });
    });
  });

}