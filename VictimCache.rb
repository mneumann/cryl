#
# A VictimCache is a cache that simply replaces an element in case of a
# collision.
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class VictimCache < Array
  def initialize(size)
    super(size)
  end

  def get(key)
    item = at(key.hash % size())
    if item and item.first == key
      item
    else
      nil
    end
  end

  def put(item) 
    key = item.first
    self[key.hash % size()] = item
  end
end
