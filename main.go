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

var (
	active    = 0
	viewNames = []string{"topic", "data"}
	breaker   = make(chan struct{})
	topic     string
	partition int
	brokers   []string
	wg        sync.WaitGroup
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
	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		log.Panicln(err)
	}
	defer g.Close()

	g.SetManagerFunc(layout)
	g.Highlight = true
	g.SelFgColor = gocui.ColorGreen

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

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
	return nil
}

func nextView(g *gocui.Gui, v *gocui.View) error {
	g.SetCurrentView(viewNames[active])
	active = (active + 1) % len(viewNames)

	return nil
}

func play(g *gocui.Gui, topic string, partition int32, offset int64, die chan struct{}) {
	wg.Add(1)
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), int64(offset))
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			g.Execute(func(g *gocui.Gui) error {
				v, _ := g.View("data")
				v.Clear()
				v.Title = fmt.Sprintf("KEY:%v OFFSET:%v TIMESTAMP:%v", string(msg.Key), msg.Offset, msg.Timestamp)
				fmt.Fprintln(v, string(msg.Value))
				return nil
			})
			//<-time.After(20 * time.Millisecond)
		case <-die:
			wg.Done()
			return
		}
	}
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
		if err := v.SetCursor(cx, cy+1); err != nil {
			ox, oy := v.Origin()
			if err := v.SetOrigin(ox, oy+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func selectTopic(g *gocui.Gui, v *gocui.View) error {
	var l string
	var err error

	_, cy := v.Cursor()
	if l, err = v.Line(cy); err != nil {
		l = ""
	}
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

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
	var l string
	var err error

	_, cy := v.Cursor()
	if l, err = v.Line(cy); err != nil {
		l = ""
	}
	close(breaker)
	breaker = make(chan struct{})
	wg.Wait()
	partition, _ = strconv.Atoi(l)
	go play(g, topic, int32(partition), sarama.OffsetOldest, breaker)

	if err := g.DeleteView("partition"); err != nil {
		return err
	}
	if _, err := g.SetCurrentView("topic"); err != nil {
		return err
	}
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	infoview, _ := g.View("info")
	infoview.Clear()
	fmt.Fprintf(infoview, "Brokers: %v\n", brokers)
	fmt.Fprintf(infoview, "Topic: %v\n", topic)
	fmt.Fprintf(infoview, "Partition: %v\n", partition)
	replicas, _ := client.Replicas(topic, int32(partition))
	fmt.Fprintf(infoview, "Replicas: %v\n", replicas)
	broker, _ := client.Leader(topic, int32(partition))
	fmt.Fprintf(infoview, "Leader: %v\n", broker.ID())
	return nil
}

func layout(g *gocui.Gui) error {
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

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
		fmt.Fprintln(v, "← → : Rewind/FastForward")
		fmt.Fprintln(v, "[: Jump to oldest")
		fmt.Fprintln(v, "]: Jump to newest")
		fmt.Fprintln(v, "Ctrl+O: Seek to Offset")
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
		fmt.Fprintln(v, "F5: Refresh Topics")
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

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}
