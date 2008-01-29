#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#

require 'test/unit'
require 'HttpUrl'

class Test_HtmlUrl < Test::Unit::TestCase
  def test_relative_without_base_url
    assert_nil parse("a/b/c")
    assert_nil parse("/a/b/c")
    assert_nil parse("?abc")
    assert_nil parse("/test?abc")
    assert_nil parse("http:test")
    assert_nil parse("http:/test")
  end

  def test_absolute
    assert_equal ['www.ntecs.de', 80, '/', nil], parse('http://www.ntecs.de').to_a
    assert_equal ['www.ntecs.de', 80, '/', nil], parse('http://www.ntecs.de:80').to_a
    assert_equal ['www.ntecs.de', 80, '/', nil], parse('http://www.ntecs.de/').to_a
    assert_equal ['www.ntecs.de', 80, '/', nil], parse('http://www.ntecs.de:80/').to_a

    assert_equal ['www.ntecs.de', 80, '/', 'query'], parse('http://www.ntecs.de?query').to_a
    assert_equal ['www.ntecs.de', 80, '/', 'query'], parse('http://www.ntecs.de/?query').to_a
  end

  def test_with_base_url
    base = parse('http://www.ntecs.de/?query')
    assert_equal ['www.ntecs.de', 80, '/a/b/c', nil], parse('a/b/c', base).to_a
    assert_equal ['www.ntecs.de', 80, '/a/b/c', nil], parse('/a/b/c', base).to_a
    assert_equal ['www.ntecs.de', 80, '/', nil], parse('', base).to_a

    base = parse('http://www.ntecs.de/d/e/f?query')
    assert_equal ['www.ntecs.de', 80, '/d/e/f', nil], parse('', base).to_a
    assert_equal ['www.ntecs.de', 80, '/d/e/f', "query2"], parse('?query2', base).to_a
    assert_equal ['www.ntecs.de', 80, '/abc', "query2"], parse('/abc?query2', base).to_a
    assert_equal ['www.ntecs.de', 80, '/abc', "query2"], parse('http://www.ntecs.de/abc?query2', base).to_a
    assert_equal ['www.ntecs.de', 80, '/d/e/gh', "query2"], parse('gh?query2', base).to_a
  end

  def test_query_sorting
    base = parse('http://www.ntecs.de/')
    assert_equal 'a=1&a=2&a=2', parse('?a=2&a=1&a=2', base).query
    assert_equal 'a=1&a=2&b=2', parse('?b=2&a=1&a=2', base).query
  end

  protected

  def parse(href, base_url=nil)
    HttpUrl.parse(href, base_url)
  end
end
