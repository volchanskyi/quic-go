package quic

import (
	"testing"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/udp"

	"github.com/stretchr/testify/require"
)

func TestReceivedPacketQueue(t *testing.T) {
	h1 := &pathReceiver{id: 1}
	h2 := &pathReceiver{id: 2}
	var q receivedPacketQueue
	q.Init(h1)

	push := func(data string, source *pathReceiver) {
		t.Helper()
		require.True(t, q.Push(receivedPacket{data: []byte(data)}, source))
	}
	requirePop := func(data string, path pathID) {
		t.Helper()
		p, id, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, data, string(p.data))
		require.Equal(t, path, id)
	}

	// packets received on other paths are returned first, with their path ID
	push("a", h1)
	push("b", h2)
	requirePop("b", 2)
	requirePop("a", activePathID)
	require.True(t, q.Empty())

	// switching paths moves the packets of the old path to the side queue
	push("c", h1)
	push("d", h2)
	require.Same(t, h1, q.SwapActive(h2))
	push("e", h2)
	push("f", h1)
	requirePop("d", activePathID) // received on the new path before the switch
	requirePop("c", 1)
	requirePop("f", 1)
	requirePop("e", activePathID)

	// switching to the active path doesn't move any packets
	push("g", h2)
	require.Same(t, h2, q.SwapActive(h2))
	push("h", h1)
	requirePop("h", 1)
	requirePop("g", activePathID)
	require.True(t, q.Empty())
}

func TestReceivedPacketQueueLimits(t *testing.T) {
	h1 := &pathReceiver{id: 1}
	h2 := &pathReceiver{id: 2}
	var q receivedPacketQueue
	q.Init(h1)

	// the main and the side queue have their own limit
	for _, h := range []*pathReceiver{h1, h2} {
		for range protocol.MaxConnUnprocessedPackets {
			require.True(t, q.Push(receivedPacket{}, h))
		}
		require.False(t, q.Push(receivedPacket{}, h))
	}

	// switching paths moves all packets, ignoring the limit of the side queue
	q.SwapActive(h2)
	var n int
	for !q.Empty() {
		q.Pop()
		n++
	}
	require.Equal(t, 2*protocol.MaxConnUnprocessedPackets, n)
}

func TestReceivedPacketQueueShrinks(t *testing.T) {
	h := &pathReceiver{id: 1}
	var q receivedPacketQueue
	q.Init(h)

	pushAndPop := func(n int) {
		t.Helper()
		for range n {
			require.True(t, q.Push(receivedPacket{}, h))
		}
		for range n {
			_, _, ok := q.Pop()
			require.True(t, ok)
		}
		require.True(t, q.Empty())
	}

	for range 100 {
		require.True(t, q.Push(receivedPacket{}, h))
	}
	require.Equal(t, 128, q.main.Cap())
	for range 99 {
		_, _, ok := q.Pop()
		require.True(t, ok)
	}
	require.Equal(t, 128, q.main.Cap())
	// the burst used most of the storage, so it is kept when the queue is drained
	_, _, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, 128, q.main.Cap())

	// a busy queue keeps its storage
	for range 10 {
		pushAndPop(33)
		require.Equal(t, 128, q.main.Cap())
	}
	// once the queue stays mostly empty, the storage is halved every time it is drained
	pushAndPop(32)
	require.Equal(t, 64, q.main.Cap())
	pushAndPop(17)
	require.Equal(t, 64, q.main.Cap())
	for _, size := range []int{32, 16, 8, 8} {
		pushAndPop(1)
		require.Equal(t, size, q.main.Cap())
	}

	require.True(t, q.Push(receivedPacket{data: []byte("foo")}, h))
	require.True(t, q.Push(receivedPacket{data: []byte("bar")}, h))
	p, _, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, "foo", string(p.data))
	p, _, ok = q.Pop()
	require.True(t, ok)
	require.Equal(t, "bar", string(p.data))
}

func TestReceivedPacketQueueClear(t *testing.T) {
	h1 := &pathReceiver{id: 1}
	h2 := &pathReceiver{id: 2}
	var q receivedPacketQueue
	q.Init(h1)

	require.True(t, q.Push(receivedPacket{data: []byte("foo"), buffer: udp.GetPacketBuffer()}, h1))
	require.True(t, q.Push(receivedPacket{data: []byte("bar"), buffer: udp.GetPacketBuffer()}, h2))
	q.Clear()
	require.True(t, q.Empty())
}
