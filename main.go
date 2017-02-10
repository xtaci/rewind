package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"

	cli "gopkg.in/urfave/cli.v2"

	"github.com/Shopify/sarama"
	"github.com/jroimartin/gocui"
)

const (
	cmdPlay = iota
	cmdBack
	cmdForward
	cmdJumpOldest
	cmdJumpNewest
)

var (
	active    = 0
	viewNames = []string{"topic", "data"}
	control   = make(chan int, 1)
	client    sarama.Client
	topic     string
	partition int
	brokers   []string
	wg        sync.WaitGroup
	stop      = make(chan struct{})
	paused    bool
)

func main() {
	app := &cli.App{
		Name:    "rewind",
		Usage:   `a kafka topic player`,
		Version: "1.0",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "brokers, b",
				Value: cli.NewStringSlice("localhost:9092"),
				Usage: "kafka brokers address",
			},
		},
		Action: processor,
	}
	app.Run(os.Args)
}

func processor(c *cli.Context) error {
	brokers = c.StringSlice("brokers")
	var err error
	client, err = sarama.NewClient(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		log.Panicln(err)
	}
	defer g.Close()

	g.SetManagerFunc(layout)
	g.Highlight = true
	g.SelFgColor = gocui.ColorMagenta

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Panicln(err)
	}
	if err := g.SetKeybinding("", gocui.KeyTab, gocui.ModNone, nextView); err != nil {
		log.Panicln(err)
	}
	if err := g.SetKeybinding("topic", gocui.KeyArrowDown, gocui.ModNone, cursorDown); err != nil {
		return err
	}
	if err := g.SetKeybinding("topic", gocui.KeyArrowUp, gocui.ModNone, cursorUp); err != nil {
		return err
	}
	if err := g.SetKeybinding("topic", gocui.KeyEnter, gocui.ModNone, selectTopic); err != nil {
		return err
	}
	if err := g.SetKeybinding("partition", gocui.KeyEnter, gocui.ModNone, selectPartition); err != nil {
		return err
	}
	if err := g.SetKeybinding("partition", gocui.KeyArrowDown, gocui.ModNone, cursorDown); err != nil {
		return err
	}
	if err := g.SetKeybinding("partition", gocui.KeyArrowUp, gocui.ModNone, cursorUp); err != nil {
		return err
	}

	if err := g.SetKeybinding("data", gocui.KeySpace, gocui.ModNone,
		func(g *gocui.Gui, v *gocui.View) error {
			select {
			case control <- cmdPlay:
			default:
			}
			return nil
		}); err != nil {
		return err
	}

	if err := g.SetKeybinding("data", gocui.KeyArrowLeft, gocui.ModNone,
		func(g *gocui.Gui, v *gocui.View) error {
			select {
			case control <- cmdBack:
			default:
			}
			return nil
		}); err != nil {
		return err
	}

	if err := g.SetKeybinding("data", gocui.KeyArrowRight, gocui.ModNone,
		func(g *gocui.Gui, v *gocui.View) error {
			select {
			case control <- cmdForward:
			default:
			}
			return nil
		}); err != nil {
		return err
	}

	if err := g.SetKeybinding("data", '[', gocui.ModNone,
		func(g *gocui.Gui, v *gocui.View) error {
			select {
			case control <- cmdJumpOldest:
			default:
			}
			return nil
		}); err != nil {
		return err
	}

	if err := g.SetKeybinding("data", ']', gocui.ModNone,
		func(g *gocui.Gui, v *gocui.View) error {
			select {
			case control <- cmdJumpNewest:
			default:
			}
			return nil
		}); err != nil {
		return err
	}

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
	return nil
}

// Model
func player(g *gocui.Gui, topic string, partition int32, offset int64) {
	wg.Add(1)
	defer wg.Done()

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), int64(offset))
	if err != nil {
		panic(err)
	}

	defer func() {
		partitionConsumer.Close()
	}()

	chMessage := partitionConsumer.Messages()
	for {
		select {
		case msg := <-chMessage:
			offset = msg.Offset
			g.Execute(func(g *gocui.Gui) error {
				v, _ := g.View("data")
				v.Clear()
				v.Title = fmt.Sprintf("OFFSET:%v KEY:%v TIMESTAMP:%v", msg.Offset, string(msg.Key), msg.Timestamp)
				fmt.Fprintln(v, string(msg.Value))
				return nil
			})
			if paused {
				chMessage = nil
			}
		case c := <-control:
			switch c {
			case cmdPlay:
				paused = !paused
				if paused {
					chMessage = nil
				} else {
					chMessage = partitionConsumer.Messages()
				}
			case cmdBack:
				if offset-1 < 0 {
					continue
				}
				paused = true
				partitionConsumer.Close()
				partitionConsumer, err = consumer.ConsumePartition(topic, int32(partition), int64(offset-1))
				if err != nil {
					panic(err)
				}
				chMessage = partitionConsumer.Messages()
			case cmdForward:
				paused = true
				partitionConsumer.Close()
				partitionConsumer, err = consumer.ConsumePartition(topic, int32(partition), int64(offset+1))
				if err != nil {
					partitionConsumer, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
					if err != nil {
						panic(err)
					}
				}
				chMessage = partitionConsumer.Messages()
			case cmdJumpOldest:
				paused = true
				partitionConsumer.Close()
				partitionConsumer, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
				if err != nil {
					panic(err)
				}
				chMessage = partitionConsumer.Messages()
			case cmdJumpNewest:
				lastOffset, err := client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
				if err != nil {
					panic(err)
				}

				paused = true
				partitionConsumer.Close()
				partitionConsumer, err = consumer.ConsumePartition(topic, int32(partition), lastOffset-1)
				if err != nil {
					panic(err)
				}
				chMessage = partitionConsumer.Messages()
			}
			refreshInfo(g)
		case <-stop:
			return
		}
	}
}

// View
func nextView(g *gocui.Gui, v *gocui.View) error {
	active = (active + 1) % len(viewNames)
	g.SetCurrentView(viewNames[active])
	return nil
}

func cursorUp(g *gocui.Gui, v *gocui.View) error {
	if v != nil {
		ox, oy := v.Origin()
		cx, cy := v.Cursor()
		if err := v.SetCursor(cx, cy-1); err != nil && oy > 0 {
			if err := v.SetOrigin(ox, oy-1); err != nil {
				return err
			}
		}
	}
	return nil
}

func cursorDown(g *gocui.Gui, v *gocui.View) error {
	if v != nil {
		cx, cy := v.Cursor()
		if l, err := v.Line(cy + 1); err == nil {
			if l == "" {
				return nil
			}
		} else {
			return err
		}

		if err := v.SetCursor(cx, cy+1); err != nil {
			ox, oy := v.Origin()
			if err := v.SetOrigin(ox, oy+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()
	if v, err := g.SetView("topic", 0, 0, 10, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "TOPIC"
		topics, err := client.Topics()
		if err != nil {
			panic(err)
		}

		sort.Slice(topics, func(i, j int) bool {
			if topics[i] < topics[j] {
				return true
			}
			return false
		})
		for k := range topics {
			fmt.Fprintln(v, topics[k])
		}
		v.Highlight = true
		v.SelBgColor = gocui.ColorGreen
		v.SelFgColor = gocui.ColorBlack
		g.SetCurrentView("topic")
	}

	if v, err := g.SetView("play", 11, maxY-10, 11+30, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "PLAY"
		fmt.Fprintln(v, "Space: Pause/Play")
		fmt.Fprintln(v, "← → : Back/Forward")
		fmt.Fprintln(v, "[: Jump to oldest")
		fmt.Fprintln(v, "]: Jump to newest")
	}

	if v, err := g.SetView("info", 11+30+1, maxY-10, 11+30+50, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "INFO"
	}

	if v, err := g.SetView("options", 11+30+50+1, maxY-10, maxX-1, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "GLOBAL OPTIONS"
		fmt.Fprintln(v, "Tab: Next View")
		fmt.Fprintln(v, "^C: Exit")
	}

	if v, err := g.SetView("data", 11, 0, maxX-1, maxY-11); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "DATA"
		v.Wrap = true
	}

	return nil
}

// Control
func selectTopic(g *gocui.Gui, v *gocui.View) error {
	var l string
	var err error

	_, cy := v.Cursor()
	if l, err = v.Line(cy); err != nil {
		l = ""
	}
	parts, err := client.Partitions(l)
	if err != nil {
		panic(err)
	}

	topic = l
	maxX, maxY := g.Size()
	if v, err := g.SetView("partition", maxX/2-10, maxY/2, maxX/2+10, maxY/2+2); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		for k := range parts {
			fmt.Fprintln(v, parts[k])
		}
		if _, err := g.SetCurrentView("partition"); err != nil {
			return err
		}
		v.Title = "Select Partition"
	}
	return nil
}

func selectPartition(g *gocui.Gui, v *gocui.View) error {
	// get partition
	var l string
	var err error

	_, cy := v.Cursor()
	if l, err = v.Line(cy); err != nil {
		l = ""
	}
	if err := g.DeleteView("partition"); err != nil {
		return err
	}
	if _, err := g.SetCurrentView("data"); err != nil {
		return err
	}
	dataView, _ := g.View("data")
	dataView.Clear()
	active = 1

	// turn off player
	close(stop)
	stop = make(chan struct{})

	// wait until player exists
	wg.Wait()

	// create new player for partition
	partition, _ = strconv.Atoi(l)
	go player(g, topic, int32(partition), sarama.OffsetNewest)

	// update info
	refreshInfo(g)
	return nil
}

func refreshInfo(g *gocui.Gui) {
	g.Execute(func(g *gocui.Gui) error {
		infoview, _ := g.View("info")
		infoview.Clear()
		fmt.Fprintf(infoview, "Brokers: %v\n", brokers)
		fmt.Fprintf(infoview, "Topic: %v\n", topic)
		fmt.Fprintf(infoview, "Partition: %v\n", partition)
		replicas, _ := client.Replicas(topic, int32(partition))
		fmt.Fprintf(infoview, "Replicas: %v\n", replicas)
		broker, _ := client.Leader(topic, int32(partition))
		fmt.Fprintf(infoview, "Leader: %v\n", broker.ID())
		if paused {
			fmt.Fprintf(infoview, "\033[31mPAUSED\033[0m\n")
		} else {
			fmt.Fprintf(infoview, "\033[32mPLAYING\033[0m\n")
		}
		return nil
	})
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}
