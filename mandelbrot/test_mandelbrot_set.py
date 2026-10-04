import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mandelbrot_set import check_c


class CheckCTest(unittest.TestCase):
    def test_origin_is_bounded(self):
        self.assertEqual(check_c(0, 0), (0, 0, 0.0))

    def test_interior_point_is_bounded(self):
        result = check_c(-1, 0)
        self.assertEqual(result[:2], (-1, 0))
        self.assertLessEqual(result[2], 2)

    def test_exterior_point_escapes(self):
        self.assertIsNone(check_c(2, 0))

    def test_similar_magnitudes_do_not_imply_bounded(self):
        self.assertIsNone(check_c(-0.7378280431141526, 0.414251297567644))


if __name__ == "__main__":
    unittest.main()
