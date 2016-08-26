context("Asserts")

test_that("Test assert Code failed", expect_match({
  depwalker:::assertCode('f(1(')
}, 'unexpected end of input'))


