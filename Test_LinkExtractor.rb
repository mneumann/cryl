#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#

require 'test/unit'
require 'rubygems'
require 'LinkExtractor'

class Test_LinkExtractor < Test::Unit::TestCase
  def test_simple
    html = %{
    <html>
      <a href="http://www.google.de/">test
      <A href="a/b/c" />
      </a>
      <a href="#" >
      <A href="#    " >
      <A href=" #abcdef    " >
      <a href="  " >
    }

    assert_equal ["http://www.google.de/", "a/b/c"], LinkExtractor.new(html).to_a
  end
end
