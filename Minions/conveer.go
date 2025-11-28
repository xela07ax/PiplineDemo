package Minions

import (
	"fmt"
	"sync"
)

// ProcessorFunc определяет сигнатуру функции обработки данных в ноде.
// Она принимает входной канал и возвращает выходной канал.

type Conveer struct {
	Name       string
	InputChan  chan interface{}
	OutputChan chan [4]interface{}                                                  // 1-данные, 2-ошибка, 3-завершение, 4-kill
	execFunc   func(gopherNumber int, element interface{}, out chan [4]interface{}) // out - Интерфейс, чтобы сделать библиотеку более гибкой.
	process    *mutexRunner
	terminate  chan struct{} // Сигнал, что пока закругляться
	kill       chan struct{} // Принудительное завершение
	stopX      chan bool     //Миньон завершила свою работу
	Wait       chan bool
}

// Сценарий завершения 1, корректное завершение
// мы получили команду завершиться, дочитываем очередь и выходим
// Сценарий завершения 2, принудительное завершение
// мы получили команду принудительного завершения, выходим незамедлительно

func (m *Conveer) Kill() {
	close(m.kill)
	for i := 0; i < m.GetCores(); i++ {
		<-m.stopX
	}
	return
}

func (m *Conveer) Stop() {
	close(m.terminate)
	for i := 0; i < m.GetCores(); i++ {
		<-m.stopX
	}
	return
}

func (m *Conveer) WaitStoper(terminate <-chan bool) {
	<-terminate
	m.Stop()
	m.Wait <- true
	return
}

func NewConveer(name string, execFunc func(gopherNumber int, element interface{}, out chan [4]interface{})) *Conveer {
	return &Conveer{
		Name:       name,
		execFunc:   execFunc,
		InputChan:  make(chan interface{}, 1000),
		OutputChan: make(chan [4]interface{}, 1000),
		terminate:  make(chan struct{}),
		stopX:      make(chan bool, 1000),
		process:    new(mutexRunner), Wait: make(chan bool),
		kill: make(chan struct{}),
	}
}

type mutexRunner struct {
	mu sync.Mutex
	x  int
}

// popRun Будем отмечать количество отключенных рутин
func (c *mutexRunner) popRun() (i int) {
	c.mu.Lock()
	c.x--
	i = c.x
	c.mu.Unlock()
	return
}

// addRun Ведем подсчет запущенных рутин
func (c *mutexRunner) addRun() (i int) {
	c.mu.Lock()
	c.x++
	i = c.x
	c.mu.Unlock()
	return
}
func (c *mutexRunner) getCores() (cores int) {
	c.mu.Lock()
	cores = c.x
	c.mu.Unlock()
	return
}

func (m *Conveer) GetCores() (cores int) {
	cores = m.process.getCores()
	return
}

func (m *Conveer) RunMinions(parallels int) {
	for i := 1; i <= parallels; i++ {
		go m.runMinion(m.process.addRun())
	}
	return
}

func (m *Conveer) runMinion(gopher int) {
	m.circle(gopher)
}

func (m *Conveer) circle(gopher int) {
	for {
		select {
		case element := <-m.InputChan:
			m.execFunc(gopher, element, m.OutputChan)
		case <-m.terminate:
			if len(m.InputChan) == 0 { // Корректно завершим оставшиеся задания
				m.process.popRun()
				m.stopX <- true
				return
			}
		case <-m.kill:
			m.process.popRun()
			fmt.Printf("Minions %s conveer %d killed, queue len %d\n", m.Name, gopher, len(m.InputChan))
			m.stopX <- true
			return
		}
	}
}
