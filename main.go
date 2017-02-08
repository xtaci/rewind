package main

import (
	"fmt"
	"log"
	"os"
	"time"

	cli "gopkg.in/urfave/cli.v2"

	"github.com/Shopify/sarama"
	"github.com/jroimartin/gocui"
)

func main() {
	app := &cli.App{
		Name:    "rewind",
		Usage:   `a kafka player like tape control`,
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

	go play(g, "test", 0, 0)
	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Panicln(err)
	}
	return nil
}

var active = 0
var viewNames = []string{"topic", "control", "content"}

func nextView(g *gocui.Gui, v *gocui.View) error {
	g.SetCurrentView(viewNames[active])
	active = (active + 1) % len(viewNames)

	return nil
}

var breaker chan struct{}

func play(g *gocui.Gui, topic string, partition int32, offset int64) {
	breaker = make(chan struct{})
	client, err := sarama.NewClient([]string{"localhost:9092"}, nil)
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
				v, err := g.View("content")
				if err != nil {
					return err
					// handle error
				}
				v.Clear()
				fmt.Fprintln(v, string(msg.Value))
				return nil
			})
			<-time.After(20 * time.Millisecond)
		case <-breaker:
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
	close(breaker)
	go play(g, l, 0, 0)
	return nil
}

func layout(g *gocui.Gui) error {
	client, err := sarama.NewClient([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

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

		for k := range topics {
			fmt.Fprintln(v, topics[k])
		}
		v.Highlight = true
		v.SelBgColor = gocui.ColorGreen
		v.SelFgColor = gocui.ColorBlack
		g.SetCurrentView("topic")
	}

	if v, err := g.SetView("control", 11, maxY-10, maxX-1, maxY-1); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "CTRL"
		fmt.Fprintln(v, "TODO: rewind/replay/fastforward")
	}

	if v, err := g.SetView("content", 11, 0, maxX-1, maxY-11); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "DATA"
		v.Wrap = true
		v.Autoscroll = true
	}

	return nil
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}
