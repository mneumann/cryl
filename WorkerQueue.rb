#
# A WorkerQueue consists of two queue, an input queue and an output
# queue. The following constraint is always true:
#
#   input_queue.size + output_queue.size <= @max_size
#
# Note, +nil+ shouldn't be used as element. Also, duplicate items
# shouldn't be used as remove_from_input removes all duplicates. 
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class WorkerQueue
  def initialize(max_size)
    @max_size = max_size
    @input_queue = Array.new
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
    raise if full? 
    @input_queue.push(item)
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

  #
  # Transfer items the output_queue of +self+ into the input_queue 
  # of +worker_queue as long as the input_queue is not full. Only
  # transfer items for which the block evalutes to +true+.
  #
  # You are not allowed to modify the worker_queues during this
  # operation!
  #
  def transfer(worker_queue)
    delete_list = []
    @output_queue.each do |item|
      break if worker_queue.full?
      if yield(item)
        worker_queue.enqueue(item)
        delete_list.push(item)
      end
    end

    delete_list.each do |item|
      @output_queue.delete(item)
    end
  end
end
