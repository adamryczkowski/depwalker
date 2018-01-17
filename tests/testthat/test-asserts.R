context("Asserts")

testthat::test_that("Test assert Code failed", testthat::expect_match({
  depwalker:::assertCode('f(1(')
}, 'unexpected end of input'))


