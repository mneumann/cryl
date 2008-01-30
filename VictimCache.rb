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
