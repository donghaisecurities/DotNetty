using System.Threading;

namespace NettyClinet.Models
{
    public class LockFreeQueue<T>
    {
        private class Node<T>
        {
            public T value;
            public Node<T> next;
        }
        private Node<T> head;
        private Node<T> tail;

        private int count;
        public LockFreeQueue()
        {
            head = new Node<T>();
            tail = head;
        }

        public int Count
        {
            get { return count; }
        }

        public void EnQueue(T item)
        {
            var node = new Node<T>();
            node.value = item;
            node.next = null;

            Node<T> tmpTail = null;
            bool isReplace = false;
            do
            {
                tmpTail = tail;
                while (tmpTail.next != null)
                {
                    tmpTail = tmpTail.next;
                }
                var result = Interlocked.CompareExchange<Node<T>>(ref tmpTail.next, node, null);
                isReplace = result != tmpTail.next;

            } while (!isReplace);

            Interlocked.Exchange<Node<T>>(ref tail, node);
            Interlocked.Increment(ref count);
        }


        public T Dequeue()
        {
            bool isReplace = false;
            Node<T> tmpHead = null;
            Node<T> oldHeadNext = null;
            do
            {
                tmpHead = head;
                oldHeadNext = tmpHead.next;
                if (oldHeadNext == null)
                {
                    return default(T);
                }
                else
                {
                    var result = Interlocked.CompareExchange<Node<T>>(ref head, oldHeadNext, tmpHead);
                    isReplace = result != oldHeadNext;
                }

            } while (!isReplace);

            Interlocked.Decrement(ref count);
            return oldHeadNext.value;
        }
    }
}
