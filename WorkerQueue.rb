#
# A WorkerQueue consists of two queue, an input queue and an output
# queue. The following constraint is always true:
#
#   input_queue.size + output_queue.size <= @max_size
#
# Note, +nil+ shouldn't be used as element. Accepts only unique items in
# it's input queue, i.e. no duplicate items are allowed.
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class WorkerQueue
  def initialize(max_size)
    @max_size = size
    @input_queue = Hash.new
    @output_queue = Array.new
  end

  def full?
    @input_queue.size + @output_queue.size >= @max_size
  end

  #
  # Empty means, there are no element in the output_queue.
  #
  def empty?
    @output_queue.empty?
  end

  #
  # Moves an item from the input_queue to the output_queue.
  #
  def complete(item)
    remove_from_input(item)
    enqueue_into_output(item)
  end

  def remove_from_input(item)
    @input_queue.delete(item) || raise
  end

  def enqueue(item)
    raise if full? or @input_queue.include?(item)
    @input_queue[item] = item
  end

  #
  # Directly enqueue +item+ into the output queue.
  #
  # This can be used for example when a processing step
  # should be skipped (maybe due to using a cached value).
  #
  def enqueue_into_output(item)
    raise if full?
    @output_queue.push(item)
  end

  #
  # Return +nil+ when output_queue is empty
  #
  def dequeue
    @output_queue.shift
  end

  #
  # Tries to fill the queue will the elements produces by
  # calling the block. Breaks the loop on +nil+.
  #
  def fill
    while not full? 
      if item = yield
        enqueue(item)
      else
        break
      end
    end
  end
end
