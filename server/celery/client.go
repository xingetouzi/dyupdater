package celery

import (
	"time"

	"github.com/gorilla/websocket"
)

type FlowerEvent struct {
	Type      string  `json:"type"`
	Hostname  string  `json:"hostname"`
	UUID      string  `json:"uuid"`
	Runtime   float64 `json:"runtime"`
	Timestamp float64 `json:"timestamp"`
}

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

// FlowerWsClient is a middleman between the websocket connection and the hub.
type FlowerWsClient struct {
	// The websocket connection.
	url     string
	conn    *websocket.Conn
	read    chan *FlowerEvent
	running bool
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *FlowerWsClient) readPump() {
	defer func() {
		c.conn.Close()
		if r := recover(); r != nil {
			log.Errorf("Websocket Error: %s", r)
		}
	}()
	// c.conn.SetReadLimit(maxMessageSize)
	// c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		message := new(FlowerEvent)
		err := c.conn.ReadJSON(message)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Errorf("error: %v", err)
				return
			}
			log.Error(err.Error())
			continue
		}
		c.read <- message
	}
}

func (c *FlowerWsClient) Run() {
	c.running = true
	retry := 2
	maxRetry := 32
	for c.running {
		log.Infof("Connect to %s", c.url)
		conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
		c.conn = conn
		if err != nil {
			log.Error(err.Error())
		} else {
			retry = 2
			c.readPump()
		}
		log.Infof("Reconnecting after %d second...", retry)
		time.Sleep(time.Duration(retry) * time.Second)
		retry *= 2
		if retry > maxRetry {
			retry = maxRetry
		}
	}
}

func (c *FlowerWsClient) Close() {
	c.running = false
	c.conn.Close()
}

//OpenWs open a websocket to read from the peer.
func OpenWs(read chan *FlowerEvent, url string) *FlowerWsClient {
	client := &FlowerWsClient{url: url, conn: nil, read: read, running: false}
	go client.Run()
	return client
}
