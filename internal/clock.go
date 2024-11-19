package internal

const (
	SLOT_UNUSED PageID = -1
)

// bufferpools simply keep an array of slots (defined by the bufferpool). It is
// the Clock's job to manage which pages are put in which slots, and to instruct
// the bufferpool on where to find requested pages.
type Clock struct {
	buffers [MAXPOOLSIZE]*clockSlot
}

// a slot in the clock algorithm.
type clockSlot struct {
	// the page number
	page PageID
	// is the page currently being used?
	// this is the heart of the clock algorithm. In a nutshell, when the page is
	// referenced, this is set to true. When the clock hand comes around to this
	// page, it is set to false. If the clock hand comes around and this is
	// false (i.e. the page has not been referenced since the last time the
	// clock hand was here), it will be booted out.
	//
	// initially set to false
	referenced bool
}

func newClockSlot() *clockSlot {
	return &clockSlot{
		page:       SLOT_UNUSED,
		referenced: false,
	}
}

func InitialClock() Clock {
	var clock Clock = Clock{}
	for i := range clock.buffers {
		clock.buffers[i] = newClockSlot()
	}
	return clock
}

// the bufferpool wants to delete a page number. It needs to know whether that
// page number is in the bufferpool's array, and if so, where it is.
//
// If the page number is found, return the slot position. Otherwise, return -1.
func (c *Clock) deletePage(page PageID) int {
	for i := range c.buffers {
		buf := c.buffers[i]
		if buf.page == page {
			buf.page = SLOT_UNUSED
			return i
		}
	}
	return -1
}

// tell the bufferpool which of its slot positions, if any, references the given
// page number. If the page number is found, return the slot position and true.
// Otherwise, return -1 and false.
func (c *Clock) findPage(page PageID) (int, bool) {
	for i := range c.buffers {
		buf := c.buffers[i]
		if buf.page == page {
			buf.referenced = true
			return i, true
		}
	}
	return -1, false
}

// execute the clock algorithm until a slot is replaced. Return the position of
// the slot of the page that was replaced in the bufferpool's array. This
// position is now dirty and the bufferpool needs to populate it with the new
// page.
func (c *Clock) freePage(page PageID) int {
	for {
		for i := range c.buffers {
			buf := c.buffers[i]
			if !buf.referenced || buf.page == SLOT_UNUSED {
				buf.page = page
				buf.referenced = true
				return i
			}
		}
	}
}

func (c *Clock) addPage(page PageID) (int, bool) {
	for i := range c.buffers {
		buf := c.buffers[i]
		if buf.page == SLOT_UNUSED {
			buf.page = page
			buf.referenced = true
			return i, true
		}
	}
	return -1, false
}
