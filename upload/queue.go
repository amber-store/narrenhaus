package upload

import "container/list"

// queue holds the content work that waits for a place among the pushes.
// Work a narinfo has asked for comes before work nobody has asked for, and
// within each kind the work that was first asked for, or that first
// arrived, comes first: work that is queued again after a failure is back
// where it stood. A queue is not safe for concurrent use; the uploader's
// lock guards it.
type queue struct {
	asked list.List // of *work, by askedAt
	plain list.List // of *work, by arrived
	seq   uint64    // the last number given out, for both orders
}

// push queues n. Work that is queued already stays where it is.
func (q *queue) push(n *work) {
	if n.in != nil {
		return
	}
	if n.arrived == 0 {
		q.seq++
		n.arrived = q.seq
	}
	if n.asked {
		insert(&q.asked, n, func(w *work) uint64 { return w.askedAt })
		return
	}
	insert(&q.plain, n, func(w *work) uint64 { return w.arrived })
}

// promote marks n as asked for by a narinfo, for good: queued, it moves
// behind the work that was asked for before it, and queued again later it
// goes there at once. Asking again changes nothing.
func (q *queue) promote(n *work) {
	if n.asked {
		return
	}
	n.asked = true
	q.seq++
	n.askedAt = q.seq
	if n.in == &q.plain {
		q.plain.Remove(n.at)
		n.in, n.at = nil, nil
		insert(&q.asked, n, func(w *work) uint64 { return w.askedAt })
	}
}

// next takes the work that is to run next out of the queue, or returns
// nil when nothing waits.
func (q *queue) next() *work {
	for _, l := range []*list.List{&q.asked, &q.plain} {
		if front := l.Front(); front != nil {
			n := l.Remove(front).(*work)
			n.in, n.at = nil, nil
			return n
		}
	}
	return nil
}

// len returns how much work waits.
func (q *queue) len() int { return q.asked.Len() + q.plain.Len() }

// insert puts n into l, which is ordered by order, ascending. The place
// is looked for from the back, where new work belongs.
func insert(l *list.List, n *work, order func(*work) uint64) {
	at := l.Back()
	for at != nil && order(at.Value.(*work)) > order(n) {
		at = at.Prev()
	}
	if at == nil {
		n.at = l.PushFront(n)
	} else {
		n.at = l.InsertAfter(n, at)
	}
	n.in = l
}
